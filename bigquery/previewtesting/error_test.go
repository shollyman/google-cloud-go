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

package previewtesting

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/bigquery/apiv2/bigquerypb"
	"cloud.google.com/go/bigquery/clientprovider"
	"github.com/googleapis/gax-go/v2/apierror"
	"google.golang.org/grpc/codes"
)

func TestErrorResponses(t *testing.T) {
	testcases := []struct {
		description  string
		fn           func(context.Context, *clientprovider.ClientProvider, clientprovider.TransportType) error
		wantErr      bool
		wantHttpCode int
		wantGrpcCode codes.Code
	}{
		{
			description: "no error",
			fn: func(ctx context.Context, cp *clientprovider.ClientProvider, tt clientprovider.TransportType) error {
				return nil
			},
			wantErr: false,
		},
		{
			description: "get_invalid_dataset",
			fn: func(ctx context.Context, cp *clientprovider.ClientProvider, tt clientprovider.TransportType) error {
				dc, err := cp.DatasetClient(ctx, tt)
				if err != nil {
					return err
				}
				_, err = dc.GetDataset(ctx, &bigquerypb.GetDatasetRequest{
					ProjectId: TestProjectID,
					DatasetId: fmt.Sprintf("THIS_DATASET_INVALID_%d", time.Now().UnixNano()),
				})
				return err

			},
			wantErr:      true,
			wantHttpCode: 404,
			wantGrpcCode: codes.NotFound,
		},
	}

	ctx := context.Background()
	for _, tc := range testcases {
		for _, tt := range []clientprovider.TransportType{clientprovider.TransportHTTP, clientprovider.TransportGRPC} {
			t.Run(fmt.Sprintf("%s_%s", tc.description, tt), func(t *testing.T) {
				err := tc.fn(ctx, TestClientProvider, tt)
				if tc.wantErr {
					if err == nil {
						t.Errorf("wanted error and got nil")
					} else {
						if aerr, ok := err.(*apierror.APIError); ok {
							if tt == clientprovider.TransportHTTP {
								gotHttp := aerr.HTTPCode()
								if gotHttp != -1 {
									if gotHttp != tc.wantHttpCode {
										t.Errorf("mismatch http code, got %d want %d", gotHttp, tc.wantHttpCode)
									}
								}
							}
							if tt == clientprovider.TransportGRPC {
								if st := aerr.GRPCStatus(); st != nil {
									t.Logf("status: %v", st)
									gotCode := st.Code()
									if gotCode != tc.wantGrpcCode {
										t.Errorf("mismatch grpc code, got %s want %s", gotCode.String(), tc.wantGrpcCode.String())
									}
								}
							}
						} else {
							t.Errorf("didnt get apierror: %+v", err)
						}
					}
				} else {
					if err != nil {
						t.Errorf("got error when none expected: %+v", err)
					}
				}
			})
		}
	}
}
