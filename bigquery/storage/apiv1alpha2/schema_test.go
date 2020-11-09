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
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/google/go-cmp/cmp"
	storagepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1alpha2"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestBQSchemaConversion(t *testing.T) {
	testCases := []struct {
		label    string
		in       bigquery.Schema
		expected *storagepb.TableSchema
	}{
		{
			label:    "nil schema",
			in:       nil,
			expected: nil,
		},
		{
			label: "basic conversion",
			in: bigquery.Schema{
				&bigquery.FieldSchema{
					Name:        "foo",
					Description: "a simple string",
					Required:    false,
					Type:        bigquery.StringFieldType,
				},
			},
			expected: &storagepb.TableSchema{
				Fields: []*storagepb.TableFieldSchema{
					{
						Name:        "foo",
						Type:        storagepb.TableFieldSchema_STRING,
						Mode:        storagepb.TableFieldSchema_NULLABLE,
						Description: "a simple string",
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		converted := BigQueryToProtoSchema(tc.in)
		if diff := cmp.Diff(converted, tc.expected, protocmp.Transform()); diff != "" {
			t.Errorf("diff in %s:\n%v", tc.label, diff)
		}
	}
}
