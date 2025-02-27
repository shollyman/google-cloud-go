// Copyright 2025 Google LLC
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

// The queryhelper package provides a simplified mechanism for interacting
// with BigQuery's query functionality.
package queryhelper

import (
	"context"
	"fmt"
	"sync"

	bigquery "cloud.google.com/go/bigquery/apiv2"
	"cloud.google.com/go/bigquery/apiv2/bigquerypb"
	bigquerystorage "cloud.google.com/go/bigquery/storage/apiv1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// QueryHelperClient exposes entrypoints for using the query helper functionality,
// like starting and resuming query jobs.
type QueryHelperClient struct {
	project    string
	jobClient  *bigquery.JobClient
	readClient *bigquerystorage.BigQueryReadClient
}

type queryHelperSettings struct {
	readClient   *bigquerystorage.BigQueryReadClient
	queryProject string
}

// QueryHelperOption is an option used for configuring a QueryHelperClient.
type QueryHelperOption func(*queryHelperSettings)

func WithQueryProject(projectID string) QueryHelperOption {
	return func(s *queryHelperSettings) {
		s.queryProject = projectID
	}
}

// WithReadClient can be used to provide a BigQueryStorage Read Client stub
// to the Query Helper.  It's use is to accelerate queries.
func WithReadClient(client *bigquerystorage.BigQueryReadClient) QueryHelperOption {
	return func(s *queryHelperSettings) {
		s.readClient = client
	}
}

// TODO: add options pattern
func NewQueryHelperClient(jobClient *bigquery.JobClient, opts ...QueryHelperOption) (*QueryHelperClient, error) {
	if jobClient == nil {
		return nil, fmt.Errorf("must provide a valid job client")
	}
	settings := &queryHelperSettings{}
	for _, o := range opts {
		o(settings)
	}

	qhc := &QueryHelperClient{
		jobClient: jobClient,
	}
	if err := qhc.mergeSettings(settings); err != nil {
		return nil, err
	}
	return qhc, nil
}

// TODO: will we support any settings that can actually cause errors at instantiation?
func (qhc *QueryHelperClient) mergeSettings(s *queryHelperSettings) error {
	if s.readClient != nil {
		qhc.readClient = s.readClient
	}
	if s.queryProject != "" {
		qhc.project = s.queryProject
	} else {
		// TODO: project detection from creds?
	}
	return nil
}

// StartQueryRequest will start a query based on an input PostQueryRequest,
// and immediately return a QueryResult for monitoring its progress.
func (qhc *QueryHelperClient) StartQueryRequest(ctx context.Context, req *bigquerypb.PostQueryRequest) *QueryResult {
	qCtx, qCancel := context.WithCancel(ctx)
	qr := &QueryResult{
		client:     qhc,
		pollCtx:    qCtx,
		pollCancel: qCancel,
		ready:      make(chan struct{}),
	}
	// start execution in a goroutine.
	go startQueryAndPoll(qr, req)
	return qr
}

// TODO: query params
func (qhc *QueryHelperClient) StartQuery(ctx context.Context, sql string) *QueryResult {
	req := &bigquerypb.PostQueryRequest{
		ProjectId: qhc.project,
		QueryRequest: &bigquerypb.QueryRequest{
			Query: sql,
			UseLegacySql: &wrapperspb.BoolValue{
				Value: false,
			},
			JobCreationMode: bigquerypb.QueryRequest_JOB_CREATION_OPTIONAL,
		},
	}
	return qhc.StartQueryRequest(ctx, req)
}

// Attach to a query job after it was already created.
func (qhc *QueryHelperClient) AttachToQueryJob(ctc context.Context, jobRef *bigquerypb.JobReference) (*QueryResult, error) {
	return nil, fmt.Errorf("Unimplemented")
}

type QueryResult struct {
	// results are created/owned by query helpers.
	client *QueryHelperClient
	// polling context, derived from the client.
	pollCtx    context.Context
	pollCancel context.CancelFunc

	// Use a lock for synchronizing information between a polling goroutine
	// and any consumption.
	mu sync.RWMutex
	// ready signals that the query has reached a final state (done or errored).
	ready chan struct{}
	// query ID info
	jobReference *bigquerypb.JobReference
	queryID      string
	// final error info.
	err error
	// Cached info to accelerate row fetching.
	schema          *bigquerypb.TableSchema
	cachedRows      []*structpb.Struct
	cachedNextToken string
}

// setupPolling is run as a goroutine to monitor a single query's execution.
func startQueryAndPoll(qr *QueryResult, req *bigquerypb.PostQueryRequest) {
	resp, err := qr.client.jobClient.Query(qr.pollCtx, req)
	if err != nil {
		// We died at setup, either due to error or context.
		// Update the QR.
		qr.mu.Lock()
		defer qr.mu.Unlock()
		qr.err = err
		close(qr.ready)
		return
	}
	// We'll have query identifiers after the first response,
	// regardless of completion.  Grab the lock and update the QR.
	qr.mu.Lock()
	if ref := resp.GetJobReference(); ref != nil {
		qr.jobReference = proto.Clone(ref).(*bigquerypb.JobReference)
	}
	if id := resp.GetQueryId(); id != "" {
		qr.queryID = id
	}
	if comp := resp.GetJobComplete(); comp != nil && comp.GetValue() {

		// we're done after first resp.
		if errs := resp.GetErrors(); errs != nil {
			// TODO: convert errors to a real go error, or store as its own field.
			qr.err = fmt.Errorf("errs in QueryResponse: %v", errs)
		}
		if schema := resp.GetSchema(); schema != nil {
			qr.schema = schema
		}
		if rows := resp.GetRows(); rows != nil {
			qr.cachedRows = rows
		}
		if nextToken := resp.GetPageToken(); nextToken != "" {
			qr.cachedNextToken = nextToken
		}
		// Mark the query done.
		close(qr.ready)
		// Unlock and return, since polling is over.
		qr.mu.Unlock()
		return
	}
	// We weren't done after the first request, so we'll switch to a polling
	// strategy.
	//
	// We still hold the lock, so dispose of it before starting polling.
	qr.mu.Unlock()
	pollJob(qr, qr.jobReference)
}

func pollJob(qr *QueryResult, ref *bigquerypb.JobReference) {
	// Setup a poll request for re-use.
	pollReq := &bigquerypb.GetQueryResultsRequest{
		ProjectId: ref.GetProjectId(),
		JobId:     ref.GetJobId(),
	}
	if loc := ref.GetLocation(); loc != nil {
		pollReq.Location = loc.Value
	}
	// Loop until done.
	for {
		if err := qr.pollCtx.Err(); err != nil {
			// Polling context expired, so we're done.
			qr.mu.Lock()
			defer qr.mu.Unlock()
			qr.err = err
			close(qr.ready)
			return
		}
		// issue poll request.
		resp, err := qr.client.jobClient.GetQueryResults(qr.pollCtx, pollReq)
		if err != nil {
			// TODO: deal with API error vs job error.
			// For now, just mark done.
			qr.mu.Lock()
			defer qr.mu.Unlock()
			qr.err = err
			close(qr.ready)
			return
		}
		if processPollResult(qr, resp) {
			return
		}
	}
}

func processPollResult(qr *QueryResult, resp *bigquerypb.GetQueryResultsResponse) bool {
	if comp := resp.GetJobComplete(); comp != nil && comp.GetValue() {
		// Lock if we're done, because we have work to do.
		qr.mu.Lock()
		defer qr.mu.Unlock()
		// Populate info in the result.
		if errs := resp.GetErrors(); errs != nil {
			qr.err = fmt.Errorf("errs in QueryResponse: %v", errs)
		}
		if schema := resp.GetSchema(); schema != nil {
			qr.schema = schema
		}
		if rows := resp.GetRows(); rows != nil {
			qr.cachedRows = rows
		}
		if token := resp.GetPageToken(); token != "" {
			qr.cachedNextToken = token
		}
		// Close the channel:
		close(qr.ready)
		return true // Signal that polling can stop.
	}
	return false
}

func (qr *QueryResult) Done() <-chan struct{} { return qr.ready }

// JobReference can be present before completion.
func (qr *QueryResult) JobReference() *bigquerypb.JobReference {
	qr.mu.RLock()
	defer qr.mu.RUnlock()
	return qr.jobReference
}

// QueryID can be present before completion.
// Returns empty string if not (yet) present.
func (qr *QueryResult) QueryID() string {
	qr.mu.RLock()
	defer qr.mu.RUnlock()
	return qr.queryID
}

// TODO: need to disambiguate job error from call error
func (qr *QueryResult) Error() error {
	return qr.err
}

// Schema is present at query completion, and will block until query is complete.
// TODO: or should we simply allow peeking?
func (qr *QueryResult) Schema(ctx context.Context) (*bigquerypb.TableSchema, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-qr.Done():
		return qr.schema, nil
	}
}
