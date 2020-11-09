// Copyright 2020 Google LLC
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

package storage // import "cloud.google.com/go/bigquery/storage/apiv1alpha2"

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"

	"cloud.google.com/go/internal/testutil"
	"cloud.google.com/go/internal/uid"

	"github.com/golang/protobuf/descriptor"
	"github.com/golang/protobuf/proto"

	storagepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1alpha2"
)

var (
	datasetIDs = uid.NewSpace("storage_test_dataset", &uid.Options{Sep: '_', Time: time.Now()})
	tableIDs   = uid.NewSpace("testtable", &uid.Options{Sep: '_', Time: time.Now()})
)

func withGRPCHeadersAssertion(t *testing.T, opts ...option.ClientOption) []option.ClientOption {
	grpcHeadersEnforcer := &testutil.HeadersEnforcer{
		OnFailure: t.Errorf,
		Checkers: []*testutil.HeaderChecker{
			testutil.XGoogClientHeaderChecker,
		},
	}
	return append(grpcHeadersEnforcer.CallOptions(), opts...)
}

func integrationClients(ctx context.Context, t *testing.T, opts ...option.ClientOption) (*BigQueryWriteClient, *bigquery.Client) {
	if testing.Short() {
		t.Skip("Integration tests skipped in short mode")
	}
	projID := testutil.ProjID()
	if projID == "" {
		t.Skip("Integration test skipped, see CONTRIBUTING.md for details")
	}
	ts := testutil.TokenSource(ctx, bigquery.Scope)
	if ts == nil {
		t.Skip("Integration tests skipped. See CONTRIBUTING.md for details")
	}
	opts = append(withGRPCHeadersAssertion(t, option.WithTokenSource(ts)), opts...)
	writeClient, err := NewBigQueryWriteClient(ctx, opts...)
	if err != nil {
		t.Fatalf("Creating BigQueryWriteClient error: %v", err)
	}

	bqClient, err := bigquery.NewClient(ctx, projID, option.WithTokenSource(ts))
	if err != nil {
		t.Fatalf("Creating bigquery.Client error: %v", err)
	}
	return writeClient, bqClient
}

func setupTestDataset(ctx context.Context, t *testing.T, bqClient *bigquery.Client) (ds *bigquery.Dataset, cleanup func(), err error) {
	dataset := bqClient.Dataset(datasetIDs.New())
	if err := dataset.Create(ctx, nil); err != nil {
		return nil, nil, err
	}
	return dataset, func() {
		if err := dataset.DeleteWithContents(ctx); err != nil {
			log.Printf("could not cleanup dataset %s: %v", dataset.DatasetID, err)
		}
	}, nil
}

func TestBareMetalStreaming(t *testing.T) {
	ctx := context.Background()
	writeClient, bqClient := integrationClients(ctx, t)
	defer writeClient.Close()
	defer bqClient.Close()
	dataset, cleanup, err := setupTestDataset(ctx, t, bqClient)
	if err != nil {
		t.Fatalf("Could not initiate test dataset: %v", err)
	}
	defer cleanup()

	testTable := dataset.Table(tableIDs.New())

	schema := bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "intval", Type: bigquery.IntegerFieldType},
	}
	if err := testTable.Create(ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		t.Fatalf("couldn't create test table %s: %v", testTable.FullyQualifiedName(), err)
	}

	req := &storagepb.CreateWriteStreamRequest{
		Parent: fmt.Sprintf("projects/%s/datasets/%s/tables/%s", testTable.ProjectID, testTable.DatasetID, testTable.TableID),
		WriteStream: &storagepb.WriteStream{
			Type: storagepb.WriteStream_COMMITTED,
		},
	}

	writeStream, err := writeClient.CreateWriteStream(ctx, req)
	if err != nil {
		t.Fatalf("Failed to create write stream: %v", err)
	}

	log.Printf("StreamID: %s", writeStream.Name)
	if writeStream.GetTableSchema() == nil {
		log.Println("WTF NO TABLE SCHEMA")
	}
	for _, v := range writeStream.GetTableSchema().GetFields() {
		log.Printf("\tName: %s Type: %s Mode: %s Description: %s", v.GetName(), v.GetType().String(), v.GetMode().String(), v.GetDescription())
	}

	stream, err := writeClient.AppendRows(ctx)
	if err != nil {
		t.Fatalf("failed to start appendrows stream: %v", err)
	}

	var wg sync.WaitGroup

	// start a writing goroutine and write two requests, then close send
	wg.Add(1)
	go func() {
		defer wg.Done()

		// some dummy data
		sampleData := []*TestMsg{
			{Name: proto.String("test"), Intval: proto.Int64(17)},
			{Name: proto.String("foo"), Intval: proto.Int64(99)},
			{Name: proto.String("bar"), Intval: proto.Int64(88)},
		}

		// Get the ProtoDescriptor for the test message
		_, desc := descriptor.ForMessage(sampleData[0])
		data := &storagepb.AppendRowsRequest_ProtoData{
			WriterSchema: &storagepb.ProtoSchema{
				ProtoDescriptor: desc,
			},
			Rows: &storagepb.ProtoRows{},
		}
		var serialized [][]byte
		for _, v := range sampleData {
			out, err := proto.Marshal(v)
			if err != nil {
				t.Fatalf("failed to serialize proto: %v", err)
			}
			serialized = append(serialized, out)
		}
		data.Rows.SerializedRows = serialized
		req := &storagepb.AppendRowsRequest{
			WriteStream: writeStream.Name,
			Rows: &storagepb.AppendRowsRequest_ProtoRows{
				ProtoRows: data,
			},
		}
		stream.Send(req)
		// Send a second request, after stripping schema
		data.WriterSchema = nil
		req = &storagepb.AppendRowsRequest{
			WriteStream: writeStream.Name,
			Rows: &storagepb.AppendRowsRequest_ProtoRows{
				ProtoRows: data,
			},
		}
		stream.Send(req)
		stream.CloseSend()
	}()

	// start a reading goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				log.Println("got EOF on recv")
				break
			}
			if err != nil {
				log.Printf("got err on recv: %v", err)
				break
			}
			log.Printf("got resp: %+v", resp)
		}
	}()

	wg.Wait()

	// verify we can see the appends via a query
	sql := fmt.Sprintf("SELECT COUNT(1) FROM `%s`.%s.%s", testTable.ProjectID, testTable.DatasetID, testTable.TableID)
	q := bqClient.Query(sql)
	it, err := q.Read(ctx)
	if err != nil {
		t.Fatalf("failed to issue validation query: %v", err)
	}
	var rowdata []bigquery.Value
	err = it.Next(&rowdata)
	if err != nil {
		t.Fatalf("error iterating validation results: %v", err)
	}
	// two batches of 3 rows each
	want := int64(6)
	if rowdata[0] != want {
		t.Errorf("count mismatch, got %v want %v", rowdata, want)
	}

}
