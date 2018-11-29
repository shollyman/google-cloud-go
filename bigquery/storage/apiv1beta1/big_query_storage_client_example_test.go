// Copyright 2018 Google LLC
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

// AUTO-GENERATED CODE. DO NOT EDIT.

package storage_test

import (
	"context"
	"io"

	"cloud.google.com/go/bigquery/storage/apiv1beta1"
	storagepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1beta1"
)

func ExampleNewBigQueryStorageClient() {
	ctx := context.Background()
	c, err := storage.NewBigQueryStorageClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleBigQueryStorageClient_CreateReadSession() {
	ctx := context.Background()
	c, err := storage.NewBigQueryStorageClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &storagepb.CreateReadSessionRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateReadSession(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleBigQueryStorageClient_ReadRows() {
	ctx := context.Background()
	c, err := storage.NewBigQueryStorageClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &storagepb.ReadRowsRequest{
		// TODO: Fill request struct fields.
	}
	stream, err := c.ReadRows(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// TODO: handle error.
		}
		// TODO: Use resp.
		_ = resp
	}
}

func ExampleBigQueryStorageClient_BatchCreateReadSessionStreams() {
	ctx := context.Background()
	c, err := storage.NewBigQueryStorageClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &storagepb.BatchCreateReadSessionStreamsRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.BatchCreateReadSessionStreams(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleBigQueryStorageClient_FinalizeStream() {
	ctx := context.Background()
	c, err := storage.NewBigQueryStorageClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &storagepb.FinalizeStreamRequest{
		// TODO: Fill request struct fields.
	}
	err = c.FinalizeStream(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleBigQueryStorageClient_SplitReadStream() {
	ctx := context.Background()
	c, err := storage.NewBigQueryStorageClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &storagepb.SplitReadStreamRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.SplitReadStream(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}
