// Copyright IBM Corp. 2016, 2025
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"context"

	"github.com/hashicorp/go-plugin/internal/plugin"
)

// GRPCControllerServer handles shutdown calls to terminate the server when the
// plugin client is closed.
type grpcControllerServer struct {
	server *GRPCServer
}

// Shutdown stops the grpc server. Plugins are notified first and given up
// to the server's ShutdownTimeout to exit before the server is forcibly
// stopped.
//
// GracefulStop cannot be called directly from this handler because it waits
// for outstanding RPCs, including the Shutdown call currently being
// served, to complete. The shutdown is therefore performed in its own
// goroutine and the response is returned immediately.
func (s *grpcControllerServer) Shutdown(ctx context.Context, _ *plugin.Empty) (*plugin.Empty, error) {
	resp := &plugin.Empty{}
	go func() { _ = s.server.Shutdown() }()
	return resp, nil
}
