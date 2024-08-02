// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package diagnostics

import (
	"context"
	"log"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// LoggingUnaryInterceptor is a gRPC client stream interceptor suitable for logging activity related to client gRPC streams.
//
// To use this with an existing client, pass the appropriate ClientOption to register this interceptor. For example, to instantiate a new client
// from the cloud.google.com/go/bigquery/storage/managedwriter package:
//
//	client, err := bigquery.NewDatasetClient(ctx, projectID, option.WithGRPCDialOption(grpc.WithUnaryInterceptor(LoggingUnaryInterceptor))
//
// Caveat: gRPC by default only allows a single interceptor, but there are specialized interceptors in the wild that
// enable chaining.
func LoggingUnaryInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if msg, ok := req.(proto.Message); ok {
		log.Printf("request(%T) [%s]: %s", req, cc.GetState().String(), protojson.Format(msg))
	}
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		log.Printf("outgoing metadata: %d keys", len(md))
		for k, v := range md {
			log.Printf("\tout %q => %q", k, strings.Join(v, "||"))
		}
	}
	var hdr, trailer metadata.MD
	opts = append(opts, grpc.Header(&hdr), grpc.Trailer(&trailer))
	err := invoker(ctx, method, req, reply, cc, opts...)
	if err != nil {
		if sErr, ok := status.FromError(err); ok {
			log.Printf("error code: %s, msg: %q", sErr.Code().String(), sErr.Message())
			for k, v := range sErr.Details() {
				log.Printf("\tdetail %d: %v", k, v)
			}
		} else {
			log.Printf("invoker error: [%T] %v", err, err)
		}
	}
	log.Printf("header size: %d", len(hdr))
	for k, v := range hdr {
		log.Printf("\theader %q: %q", k, strings.Join(v, "|"))
	}
	log.Printf("trailer size: %d", len(trailer))
	for k, v := range trailer {
		log.Printf("\ttrailer %q: %q", k, strings.Join(v, "|"))
	}
	log.Printf("reply(%T) [%s]: %v", reply, cc.GetState().String(), reply)
	return err
}
