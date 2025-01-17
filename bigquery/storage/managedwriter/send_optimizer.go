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
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// sendOptimizer handles the general task of optimizing AppendRowsRequest messages send to the backend.
//
// The general premise is that the ordering of AppendRowsRequests on a connection provides some opportunities
// to reduce payload size, thus potentially increasing throughput.  Care must be taken, however, as deep inspection
// of requests is potentially more costly (in terms of CPU usage) than gains from reducing request sizes.
type sendOptimizer interface {
	// signalReset is used to signal to the optimizer that the connection is freshly (re)opened, or that a previous
	// send yielded an error.
	signalReset()

	// optimizeSend handles possible manipulation of a request, and triggers the send.
	optimizeSend(arc storagepb.BigQueryWrite_AppendRowsClient, pw *pendingWrite) error
}

// verboseOptimizer is a primarily a testing optimizer that always sends the full request.
type verboseOptimizer struct {
}

func (vo *verboseOptimizer) signalReset() {
	// This optimizer is stateless.
}

// optimizeSend populates a full request every time.
func (vo *verboseOptimizer) optimizeSend(arc storagepb.BigQueryWrite_AppendRowsClient, pw *pendingWrite) error {
	return arc.Send(pw.constructFullRequest(true))
}

func (vo *verboseOptimizer) isMultiplexing() bool {
	// we declare this no to ensure we always reconnect on schema changes.
	return false
}

// exclusiveOptimizer is used for connections that service only a since exclusive stream.
// Exclusive streams are user-created (pending, committed, buffered).
//
// The optimizations here are straightforward:
// * The first request on a connection is unmodified.
// * Subsequent requests can redact WriteStream, WriterSchema, and TraceID.
type exclusiveOptimizer struct {
	haveSent bool
}

func (eo *exclusiveOptimizer) signalReset() {
	eo.haveSent = false
}

func (eo *exclusiveOptimizer) optimizeSend(arc storagepb.BigQueryWrite_AppendRowsClient, pw *pendingWrite) error {
	var err error
	if eo.haveSent {
		// subsequent send, we can send the request unmodified.
		err = arc.Send(pw.req)
	} else {
		// first request, build a full request.
		err = arc.Send(pw.constructFullRequest(true))
	}
	eo.haveSent = err == nil
	return err
}

// multiplexOpimizer is used for connections for default streams.
// This optimizer supports connections that host one or more (aka multiplexed) default stream writes.
//
// The connection for servicing default streams will observe schema updates without reconnect.
//
// In this case, the optimizations are as follows:
// * We send the WriteStream on all requests.
// * For sequential requests to the same stream, schema can be redacted after the first request.
// * Trace ID can be redacted from all requests after the first.
//
// Schema evolution is simply a case of sending the new WriterSchema as part of the request(s).  No explicit
// reconnection is necessary.
type multiplexOptimizer struct {
	// keyed by write stream.
	streamMap map[string]*versionedTemplate
}

func (mo *multiplexOptimizer) signalReset() {
	mo.streamMap = make(map[string]*versionedTemplate)
}

func (mo *multiplexOptimizer) optimizeSend(arc storagepb.BigQueryWrite_AppendRowsClient, pw *pendingWrite) error {
	streamID := pw.writeStreamID
	req := pw.req
	// Ensure WriteStream is set.
	req.WriteStream = streamID
	if tmpl, ok := mo.streamMap[streamID]; ok {
		// We've sent writes to this stream before.  Check we're still compatible.
		if !tmpl.Compatible(pw.reqTmpl) {
			// There's been a change, send a full request.
			req = pw.constructFullRequest(true)
			// Update the template entry for this stream.
			mo.streamMap[streamID] = pw.reqTmpl
		}
	} else {
		// Capture the template for subsequent sends.
		mo.streamMap[streamID] = pw.reqTmpl
	}

	err := arc.Send(req)
	if err != nil {
		mo.signalReset()
	}
	return err
}

// versionedTemplate is used for faster comparison of the templated part of
// an AppendRowsRequest, which bears settings-like fields related to schema
// and default value configuration.  Direct proto comparison through something
// like proto.Equal is far too expensive, so versionTemplate leverages a faster
// hash-based comparison to avoid the deep equality checks.
type versionedTemplate struct {
	versionTime time.Time
	hashVal     uint32
	tmpl        *storagepb.AppendRowsRequest
}

func newVersionedTemplate() *versionedTemplate {
	vt := &versionedTemplate{
		versionTime: time.Now(),
		tmpl:        &storagepb.AppendRowsRequest{},
	}
	vt.computeHash()
	return vt
}

// computeHash is an internal utility function for calculating the hash value
// for faster comparison.
func (vt *versionedTemplate) computeHash() {
	buf := new(bytes.Buffer)
	if b, err := proto.Marshal(vt.tmpl); err == nil {
		buf.Write(b)
	} else {
		// if we fail to serialize the proto (unlikely), consume the timestamp for input instead.
		binary.Write(buf, binary.LittleEndian, vt.versionTime.UnixNano())
	}
	vt.hashVal = crc32.ChecksumIEEE(buf.Bytes())
}

type templateRevisionF func(m *storagepb.AppendRowsRequest)

// revise makes a new versionedTemplate from the existing template, applying any changes.
// The original revision is returned if there's no effective difference after changes are
// applied.
func (vt *versionedTemplate) revise(changes ...templateRevisionF) *versionedTemplate {
	before := vt
	if before == nil {
		before = newVersionedTemplate()
	}
	if len(changes) == 0 {
		// if there's no changes, return the base revision immediately.
		return before
	}
	out := &versionedTemplate{
		versionTime: time.Now(),
		tmpl:        proto.Clone(before.tmpl).(*storagepb.AppendRowsRequest),
	}
	for _, r := range changes {
		r(out.tmpl)
	}
	out.computeHash()
	if out.Compatible(before) {
		// The changes didn't yield an measured difference.  Return the base revision to avoid
		// possible connection churn from no-op revisions.
		return before
	}
	return out
}

// Compatible is effectively a fast equality check, that relies on the hash value
// and avoids the potentially very costly deep comparison of the proto message templates.
func (vt *versionedTemplate) Compatible(other *versionedTemplate) bool {
	if other == nil {
		return vt == nil
	}
	return vt.hashVal == other.hashVal
}

func reviseProtoSchema(newSchema *descriptorpb.DescriptorProto) templateRevisionF {
	return func(m *storagepb.AppendRowsRequest) {
		if m != nil {
			m.Rows = &storagepb.AppendRowsRequest_ProtoRows{
				ProtoRows: &storagepb.AppendRowsRequest_ProtoData{
					WriterSchema: &storagepb.ProtoSchema{
						ProtoDescriptor: proto.Clone(newSchema).(*descriptorpb.DescriptorProto),
					},
				},
			}
		}
	}
}

func reviseMissingValueInterpretations(vi map[string]storagepb.AppendRowsRequest_MissingValueInterpretation) templateRevisionF {
	return func(m *storagepb.AppendRowsRequest) {
		if m != nil {
			m.MissingValueInterpretations = vi
		}
	}
}

func reviseDefaultMissingValueInterpretation(def storagepb.AppendRowsRequest_MissingValueInterpretation) templateRevisionF {
	return func(m *storagepb.AppendRowsRequest) {
		if m != nil {
			m.DefaultMissingValueInterpretation = def
		}
	}
}
