// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fuse

// Airtight drain requires that no reader is ever parked in an uninterruptible
// blocking read on the FUSE device. Readers wait via poll(2) (see
// waitForRequest) so a drain can wake them, which only works with a single
// reader: with multiple readers a loser of the post-poll read race would block
// directly in the device read again. Single reader serializes the (cheap) read
// syscall; request processing still runs concurrently in separate goroutines.
const useSingleReader = true

func (ms *Server) write(req *request) Status {
	if req.outPayloadSize() == 0 {
		err := handleEINTR(func() error {
			_, err := writev(ms.mountFd, [][]byte{req.outHeaderBuf, req.outDataBuf})
			return err
		})
		return ToStatus(err)
	}
	if req.readResult != nil {
		defer req.readResult.Done()
		if ms.canSplice {
			err := ms.trySplice(req, req.readResult)
			if err == nil {
				return OK
			}
			if err != errRecoverSplice {
				ms.opts.Logger.Println("trySplice:", err)
			}
		}

		req.outPayload, req.status = req.readResult.Bytes(req.outPayload)
		req.serializeHeader(len(req.outPayload))
	}

	_, err := writev(ms.mountFd, [][]byte{req.outHeaderBuf, req.outDataBuf, req.outPayload})
	return ToStatus(err)
}
