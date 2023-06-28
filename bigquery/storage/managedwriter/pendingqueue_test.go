// Copyright 2023 Google LLC
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

package managedwriter

import (
	"context"
	"testing"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
)

func TestPendingQueue_Global(t *testing.T) {
	pq := newPendingQueue(false, 5)
	if got := len(pq.listDests()); got > 0 {
		t.Errorf("expected no destinations, got %d", got)
	}
	pw := newPendingWrite(context.Background(), nil, &storagepb.AppendRowsRequest{}, nil, "foo-stream", "")
	pw2 := newPendingWrite(context.Background(), nil, &storagepb.AppendRowsRequest{}, nil, "bar-stream", "")
	pw3 := newPendingWrite(context.Background(), nil, &storagepb.AppendRowsRequest{}, nil, "baz-stream", "")

	// add first write and verify state.
	if err := pq.enqueue(pw); err != nil {
		t.Errorf("enqueue err: %v", err)
	}
	dests := pq.listDests()
	if got := len(dests); got != 1 {
		t.Errorf("expected 1 destination, got %d", got)
	}
	if got := dests[globalQueueId]; got != 1 {
		t.Errorf("expected global queue to have 1 element, got %d", got)
	}

	// enqueue more writes, verify state.
	if err := pq.enqueue(pw2); err != nil {
		t.Errorf("enqueue err: %v", err)
	}
	if err := pq.enqueue(pw3); err != nil {
		t.Errorf("enqueue err: %v", err)
	}
	dests = pq.listDests()
	if got := dests[globalQueueId]; got != 3 {
		t.Errorf("expected global queue to have 3 elements, got %d", got)
	}

	// dequeue first message
	<-pq.msgWaiting()
	gotWrite, err := pq.dequeue("blah")
	if err != nil {
		t.Errorf("error dequeueing: %v", err)
	}
	if want := "foo-stream"; gotWrite.writeStreamID != want {
		t.Errorf("wanted stream %q, got stream %q", want, gotWrite.writeStreamID)
	}
	dests = pq.listDests()
	if got := dests[globalQueueId]; got != 2 {
		t.Errorf("expected global queue to have 2 elements, got %d", got)
	}

	// dequeue second message
	<-pq.msgWaiting()
	gotWrite, err = pq.dequeue("")
	if err != nil {
		t.Errorf("error dequeueing: %v", err)
	}
	if want := "bar-stream"; gotWrite.writeStreamID != want {
		t.Errorf("wanted stream %q, got stream %q", want, gotWrite.writeStreamID)
	}
	dests = pq.listDests()
	if got := dests[globalQueueId]; got != 1 {
		t.Errorf("expected global queue to have 2 elements, got %d", got)
	}

	// dequeue third and final message
	<-pq.msgWaiting()
	gotWrite, err = pq.dequeue("")
	if err != nil {
		t.Errorf("error dequeueing: %v", err)
	}
	if want := "baz-stream"; gotWrite.writeStreamID != want {
		t.Errorf("wanted stream %q, got stream %q", want, gotWrite.writeStreamID)
	}
	dests = pq.listDests()
	if got := len(dests); got != 0 {
		t.Errorf("expected queue to be empty, has %d dests", got)
	}

	// check channel semantics
	select {
	case _, ok := <-pq.msgWaiting():
		t.Errorf("got message, expected to wait")
		if !ok {
			t.Errorf("channel closed unexpectedly")
		}
	default:
	}
	pq.close()
	if _, ok := <-pq.msgWaiting(); ok {
		t.Errorf("expected channel to be closed")
	}
}

func TestPendingQueue_Multiple(t *testing.T) {
	pq := newPendingQueue(true, 5)
	if got := len(pq.listDests()); got > 0 {
		t.Errorf("expected no destinations, got %d", got)
	}
	pw := newPendingWrite(context.Background(), nil, &storagepb.AppendRowsRequest{TraceId: "first"}, nil, "foo-stream", "")
	pw2 := newPendingWrite(context.Background(), nil, &storagepb.AppendRowsRequest{}, nil, "bar-stream", "")
	pw3 := newPendingWrite(context.Background(), nil, &storagepb.AppendRowsRequest{TraceId: "second"}, nil, "foo-stream", "")
	pw4 := newPendingWrite(context.Background(), nil, &storagepb.AppendRowsRequest{}, nil, "baz-stream", "")

	// check that dequeueing errors as expected
	stream := "foo-stream"
	if _, err := pq.dequeue(stream); err == nil {
		t.Errorf("dequeueing succeeded for empty queue")
	}

	// add first write and verify state.
	if err := pq.enqueue(pw); err != nil {
		t.Errorf("enqueue err: %v", err)
	}
	dests := pq.listDests()
	if got := len(dests); got != 1 {
		t.Errorf("expected 1 destination, got %d", got)
	}
	if got := dests[stream]; got != 1 {
		t.Errorf("expected %q queue to have 1 element, got %d", stream, got)
	}

	// add second write and verify state.
	if err := pq.enqueue(pw2); err != nil {
		t.Errorf("enqueue err: %v", err)
	}
	dests = pq.listDests()
	if got := len(dests); got != 2 {
		t.Errorf("expected 2 destination, got %d", got)
	}
	stream = "bar-stream"
	if got := dests[stream]; got != 1 {
		t.Errorf("expected %q queue to have 1 element, got %d", stream, got)
	}

	// add second write to first destination
	if err := pq.enqueue(pw3); err != nil {
		t.Errorf("enqueue err: %v", err)
	}
	dests = pq.listDests()
	if got := len(dests); got != 2 {
		t.Errorf("expected 2 destination, got %d", got)
	}
	stream = "foo-stream"
	if got := dests[stream]; got != 2 {
		t.Errorf("expected %q queue to have 2 element, got %d", stream, got)
	}

	// consume notifications and dequeue
	<-pq.msgWaiting()
	gotWrite, err := pq.dequeue(stream)
	if err != nil {
		t.Errorf("error dequeueing: %v", err)
	}
	if want := "first"; gotWrite.req.GetTraceId() != want {
		t.Errorf("dequeued write incorrect: %v", gotWrite.req)
	}
	<-pq.msgWaiting()
	gotWrite, err = pq.dequeue(stream)
	if err != nil {
		t.Errorf("error dequeueing: %v", err)
	}
	if want := "second"; gotWrite.req.GetTraceId() != want {
		t.Errorf("dequeued second write incorrect: %v", gotWrite.req)
	}

	// close, then attempt a final enqueue
	pq.close()
	if err := pq.enqueue(pw4); err == nil {
		t.Errorf("enqueue succeeded after close")
	}
	dests = pq.listDests()
	if got := len(dests); got != 1 {
		t.Errorf("expected 2 destination, got %d", got)
	}
	stream = "bar-stream"
	if got := dests[stream]; got != 1 {
		t.Errorf("expected %q queue to have 1 element, got %d", stream, got)
	}

}
