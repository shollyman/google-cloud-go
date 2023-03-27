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
	"fmt"
	"time"
	"unsafe"

	"golang.org/x/exp/slog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func DebugStreamLogger(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	dcs := &debugClientStream{
		ctx:    ctx,
		method: method,
		id:     fmt.Sprintf("debug_stream_%d", time.Now().UnixNano()), // TODO: real uuid,
	}
	log := dcs.log(slog.LevelInfo, "intercepting ClientStream")
	defer log.log()

	real, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		log.addAttr(slog.Any("Error", err))
		return nil, err
	}
	dcs.real = real
	return dcs, nil
}

const (
	clientStreamMethodName = "ClientStreamMethod"
)

type debugClientStream struct {
	ctx    context.Context
	real   grpc.ClientStream
	id     string
	method string
}

type logData struct {
	ctx   context.Context
	level slog.Level
	msg   string
	attrs []slog.Attr
}

func (ld *logData) log() {
	slog.LogAttrs(ld.ctx, ld.level, ld.msg, ld.attrs...)
}

func (ld *logData) addAttr(attr slog.Attr) {
	ld.attrs = append(ld.attrs, attr)
}

func (dcs *debugClientStream) log(level slog.Level, msg string) *logData {
	return &logData{
		level: level,
		msg:   msg,
		attrs: []slog.Attr{
			slog.String("DebugID", dcs.id),
			slog.String("Method", dcs.method),
		},
	}
}

func (dcs *debugClientStream) Header() (metadata.MD, error) {
	log := dcs.log(slog.LevelInfo, "ClientStream event")
	defer log.log()
	log.addAttr(slog.String(clientStreamMethodName, "Header"))
	resp, err := dcs.real.Header()
	if err != nil {
		log.addAttr(slog.Any("Error", err))
	}
	return resp, err
}

func (dcs *debugClientStream) Trailer() metadata.MD {
	log := dcs.log(slog.LevelInfo, "ClientStream event")
	defer log.log()
	log.addAttr(slog.String(clientStreamMethodName, "Trailer"))
	return dcs.real.Trailer()
}

func (dcs *debugClientStream) CloseSend() error {
	log := dcs.log(slog.LevelInfo, "ClientStream event")
	defer log.log()
	log.addAttr(slog.String(clientStreamMethodName, "CloseSend"))
	err := dcs.real.CloseSend()
	if err != nil {
		log.addAttr(slog.Any("Error", err))
	}
	return err
}

func (dcs *debugClientStream) Context() context.Context {
	log := dcs.log(slog.LevelInfo, "ClientStream event")
	defer log.log()
	log.addAttr(slog.String(clientStreamMethodName, "Context"))
	return dcs.real.Context()
}

func (dcs *debugClientStream) SendMsg(m interface{}) error {
	log := dcs.log(slog.LevelInfo, "ClientStream event")
	defer log.log()
	log.addAttr(slog.String(clientStreamMethodName, "SendMsg"))
	log.addAttr(slog.Int64("MessageSize", int64(unsafe.Sizeof(m))))
	err := dcs.real.SendMsg(m)
	if err != nil {
		log.addAttr(slog.Any("Error", err))
	}
	return err
}

func (dcs *debugClientStream) RecvMsg(m interface{}) error {
	log := dcs.log(slog.LevelInfo, "ClientStream event")
	defer log.log()
	log.addAttr(slog.String(clientStreamMethodName, "RecvMsg"))
	log.addAttr(slog.Int64("MessageSize", int64(unsafe.Sizeof(m))))
	err := dcs.real.RecvMsg(m)
	if err != nil {
		log.addAttr(slog.Any("Error", err))
	}
	return err
}
