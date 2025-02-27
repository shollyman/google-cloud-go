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

package queryhelper

import (
	"context"
	"testing"
	"time"

	bigquery "cloud.google.com/go/bigquery/apiv2"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestQueryRunner(t *testing.T) {
	ctx := context.Background()
	jc, err := bigquery.NewJobClient(ctx,
		option.WithEndpoint("grpc-bigquery.googleapis.com:443"),
	)
	if err != nil {
		t.Fatalf("NewJobClient: %v", err)
	}
	queryHelper := &QueryHelperClient{
		project:   "shollyman-testing",
		jobClient: jc,
	}

	for _, tc := range []struct {
		desc    string
		sql     string
		wantErr bool // TODO: need an RPC error matcher to improve this.
	}{
		{
			desc: "simple",
			sql:  "SELECT 17 as foo",
		},
		{
			// run the first query again so we see the effects of warmup.
			desc: "simple_again",
			sql:  "SELECT 17 as foo",
		},
		{
			desc: "non-deterministic",
			sql:  "SELECT CURRENT_TIMESTAMP as ts, SESSION_USER() as whoami",
		},
		{
			desc:    "invalid sql",
			sql:     "HI FRIENDS",
			wantErr: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tcCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			start := time.Now()
			t.Logf("creating query with sql: %q", tc.sql)
			runner := queryHelper.StartQuery(tcCtx, tc.sql)
			t.Logf("runner took %v to instantiate", time.Since(start))
			<-runner.Done() // wait for the query to finish
			t.Logf("runner took %v to complete query", time.Since(start))
			gotErr := runner.Error()
			if tc.wantErr {
				if gotErr == nil {
					t.Errorf("expected error but got success")
				} else {
					t.Logf("got error as expected: %v", gotErr)
				}
			} else {
				if gotErr != nil {
					t.Errorf("got error when success expected: %v", gotErr)
					return
				}
			}
			t.Logf("ID: JobRef %q QueryID %q", runner.JobReference(), runner.QueryID())
			schema, err := runner.Schema(tcCtx)
			if err != nil {
				t.Errorf("Schema() error: %v", err)
			}
			if schema != nil {
				t.Logf("schema: %s", protojson.Format(schema))
			}
			// TODO: sort out row iterator
			if rows := runner.cachedRows; rows != nil {
				t.Logf("cached %d rows", len(rows))
				for k, r := range runner.cachedRows {
					t.Logf("row %d: %q", k, protojson.Format(r))
				}
			}
		})
	}
}
