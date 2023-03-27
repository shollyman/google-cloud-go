// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package managedwriter

import (
	"context"

	"golang.org/x/exp/slog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func DebugStreamLogger(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	slog.Info("opening client stream",
		slog.String("method", method))

	real, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		slog.Info("failed to intercept client stream")
		return nil, err
	}
	return &debugClientStream{
		real: real,
	}, nil

}

type debugClientStream struct {
	real grpc.ClientStream
}

func (dcs *debugClientStream) Header() (metadata.MD, error) {
	slog.Info("called Header")
	return dcs.real.Header()
}

func (dcs *debugClientStream) Trailer() metadata.MD {
	slog.Info("called Trailer")
	return dcs.real.Trailer()
}

func (dcs *debugClientStream) CloseSend() error {
	slog.Info("called CloseSend")
	return dcs.real.CloseSend()
}

func (dcs *debugClientStream) Context() context.Context {
	slog.Info("called Context")
	return dcs.real.Context()
}

func (dcs *debugClientStream) SendMsg(m interface{}) error {
	slog.Info("called SendMsg")
	return dcs.real.SendMsg(m)
}

func (dcs *debugClientStream) RecvMsg(m interface{}) error {
	slog.Info("called RecvMsg")
	return dcs.real.RecvMsg(m)
}
