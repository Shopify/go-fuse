// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fuse

import "context"

// DrainInflight marks the server as draining and waits for requests that have
// already been read from the FUSE connection to finish processing.
func (ms *Server) DrainInflight(ctx context.Context) error {
	ms.draining.Store(true)

	done := make(chan struct{})
	go func() {
		ms.inflight.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
