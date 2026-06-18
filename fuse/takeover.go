// Package-local addition to go-fuse for graceful FUSE daemon hand-off.
// Vendored fork addition (upstream candidate). See worldfused D brief.
package fuse

import (
	"log"
	"runtime"
	"strings"
	"unsafe"
)

// MountFd returns the open /dev/fuse file descriptor backing this server's
// mount. Exposed to support graceful daemon hand-off: the fd can be kept alive
// (e.g. via the systemd fd store, or a dup) across a process restart so the
// kernel mount is not disconnected, and a replacement process can resume
// serving it with NewServerOnInitedFd.
func (ms *Server) MountFd() int {
	return ms.mountFd
}

// NegotiatedMaxWrite returns the MaxWrite this server agreed with the kernel
// during FUSE_INIT (opts.MaxWrite as echoed back by doInit). Persist it across
// a hand-off and pass it to NewServerOnInitedFd so the resumed server sizes its
// read buffers to the value the live connection actually negotiated, even if
// the replacement process's opts default has since changed.
func (ms *Server) NegotiatedMaxWrite() int {
	return ms.opts.MaxWrite
}

// NewServerOnInitedFd creates a Server that resumes serving an EXISTING,
// already-FUSE_INIT-negotiated /dev/fuse connection on fd. Unlike NewServer it
// performs no mount(2)/fusermount, and it does NOT perform the FUSE_INIT
// handshake (the kernel sends INIT only once per connection). The caller
// supplies the previously negotiated kernel settings (see
// Server.KernelSettings); pass nil to assume protocol defaults. negotiatedMaxWrite
// is the MaxWrite the prior daemon agreed with the kernel (see
// Server.NegotiatedMaxWrite); when > 0 the read buffers are sized to it rather
// than to opts, so resuming with a different opts default cannot under-size the
// buffer and EINVAL the kernel's next large WRITE. Pass 0 to size from opts.
//
// The caller owns fd; it must refer to an already-mounted, already-initialized
// FUSE connection (for example one kept alive across a daemon restart). Call
// Serve() to begin reading. Unmount() is a no-op on the returned server (it has
// no known mountpoint); unmount via the real mountpoint out of band.
func NewServerOnInitedFd(fs RawFileSystem, fd int, opts *MountOptions, settings *InitIn, negotiatedMaxWrite int) (*Server, error) {
	if opts == nil {
		opts = &MountOptions{MaxBackground: _DEFAULT_BACKGROUND_TASKS}
	}
	o := *opts
	if o.Logger == nil {
		o.Logger = log.Default()
	}
	if o.MaxWrite < 0 {
		o.MaxWrite = 0
	}
	if o.MaxWrite == 0 {
		o.MaxWrite = defaultMaxWrite
	}
	kernelMaxWrite := getMaxWrite()
	if o.MaxWrite > kernelMaxWrite {
		o.MaxWrite = kernelMaxWrite
	}
	// Pin the read buffer to the MaxWrite the connection actually negotiated
	// (the prior daemon's value), not this process's opts default. A deploy that
	// lowered the opts default would otherwise size the read buffer below what
	// the kernel may still WRITE (up to the negotiated size) and overflow → EINVAL.
	if negotiatedMaxWrite > 0 {
		o.MaxWrite = negotiatedMaxWrite
		if o.MaxWrite > kernelMaxWrite {
			o.MaxWrite = kernelMaxWrite
		}
	}
	if o.MaxStackDepth == 0 {
		o.MaxStackDepth = 1
	}
	if o.Name == "" {
		name := fs.String()
		l := len(name)
		if l > _MAX_NAME_LEN {
			l = _MAX_NAME_LEN
		}
		o.Name = strings.Replace(name[:l], ",", ";", -1)
	}

	for _, s := range []struct {
		flag bool
		mask uint64
	}{
		{o.SyncRead, CAP_ASYNC_READ},
		{o.DisableReadDirPlus, CAP_READDIRPLUS},
		{!o.IDMappedMount, CAP_ALLOW_IDMAP},
	} {
		if s.flag {
			o.DisabledCapabilities |= s.mask
		}
	}

	maxReaders := runtime.GOMAXPROCS(0)
	if maxReaders < minMaxReaders {
		maxReaders = minMaxReaders
	} else if maxReaders > maxMaxReaders {
		maxReaders = maxMaxReaders
	}

	ms := &Server{
		protocolServer: protocolServer{
			fileSystem:  fs,
			retrieveTab: make(map[uint64]*retrieveCacheRequest),
			opts:        &o,
		},
		opts:         &o,
		maxReaders:   maxReaders,
		singleReader: useSingleReader,
		ready:        make(chan error, 1),
	}
	ms.protocolServer.writev = ms.writev
	ms.reqPool.New = func() interface{} {
		return &requestAlloc{
			request: request{
				cancel: make(chan struct{}),
			},
		}
	}
	ms.readPool.New = func() interface{} {
		targetSize := o.MaxWrite + int(maxInputSize)
		if targetSize < _FUSE_MIN_READ_BUFFER {
			targetSize = _FUSE_MIN_READ_BUFFER
		}
		buf := make([]byte, targetSize+logicalBlockSize)
		buf = alignSlice(buf, unsafe.Sizeof(WriteIn{}), logicalBlockSize, uintptr(targetSize))
		return buf
	}

	// Adopt the inherited, already-initialized connection. No mount(), no INIT.
	ms.mountFd = fd
	if settings != nil {
		ms.kernelSettings = *settings
	}
	if ms.kernelSettings.Minor >= 13 {
		ms.setSplice()
	}
	ms.fileSystem.Init(ms)

	if err := ms.setupDrainWake(); err != nil {
		return nil, err
	}

	// Prepare for Serve() being called, mirroring NewServer.
	ms.loops.Add(1)
	return ms, nil
}
