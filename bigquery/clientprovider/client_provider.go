// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package clientprovider

import (
	"context"
	"fmt"

	bigquery "cloud.google.com/go/bigquery/apiv2"
	"google.golang.org/api/option"
)

type ClientProvider struct {
	defOpts []option.ClientOption
}

type TransportType string

const (
	TransportHTTP TransportType = "HTTP"
	TransportGRPC TransportType = "GRPC"
)

// Create a new client provider that can produce clients for BigQuery.  The opts provided
// here are stored as default options that are passed to each client instantiation.
func NewClientProvider(opts ...option.ClientOption) (*ClientProvider, error) {
	return &ClientProvider{defOpts: opts}, nil
}

func (cp *ClientProvider) DatasetClient(ctx context.Context, typ TransportType, opts ...option.ClientOption) (*bigquery.DatasetClient, error) {
	merged := cp.defOpts
	merged = append(merged, opts...)
	switch typ {
	case TransportHTTP:
		return bigquery.NewDatasetRESTClient(ctx, merged...)
	case TransportGRPC:
		return bigquery.NewDatasetClient(ctx, merged...)
	default:
		return nil, fmt.Errorf("Invalid transport type: %q", typ)
	}
}

func (cp *ClientProvider) JobClient(ctx context.Context, typ TransportType, opts ...option.ClientOption) (*bigquery.JobClient, error) {
	merged := cp.defOpts
	merged = append(merged, opts...)
	switch typ {
	case TransportHTTP:
		return bigquery.NewJobRESTClient(ctx, merged...)
	case TransportGRPC:
		return bigquery.NewJobClient(ctx, merged...)
	default:
		return nil, fmt.Errorf("Invalid transport type: %q", typ)
	}
}

func (cp *ClientProvider) TableClient(ctx context.Context, typ TransportType, opts ...option.ClientOption) (*bigquery.TableClient, error) {
	merged := cp.defOpts
	merged = append(merged, opts...)
	switch typ {
	case TransportHTTP:
		return bigquery.NewTableRESTClient(ctx, merged...)
	case TransportGRPC:
		return bigquery.NewTableClient(ctx, merged...)
	default:
		return nil, fmt.Errorf("Invalid transport type: %q", typ)
	}
}

func (cp *ClientProvider) RoutineClient(ctx context.Context, typ TransportType, opts ...option.ClientOption) (*bigquery.RoutineClient, error) {
	merged := cp.defOpts
	merged = append(merged, opts...)
	switch typ {
	case TransportHTTP:
		return bigquery.NewRoutineRESTClient(ctx, merged...)
	case TransportGRPC:
		return bigquery.NewRoutineClient(ctx, merged...)
	default:
		return nil, fmt.Errorf("Invalid transport type: %q", typ)
	}
}

func (cp *ClientProvider) ModelClient(ctx context.Context, typ TransportType, opts ...option.ClientOption) (*bigquery.ModelClient, error) {
	merged := cp.defOpts
	merged = append(merged, opts...)
	switch typ {
	case TransportHTTP:
		return bigquery.NewModelRESTClient(ctx, merged...)
	case TransportGRPC:
		return bigquery.NewModelClient(ctx, merged...)
	default:
		return nil, fmt.Errorf("Invalid transport type: %q", typ)
	}
}

func (cp *ClientProvider) ProjectClient(ctx context.Context, typ TransportType, opts ...option.ClientOption) (*bigquery.ProjectClient, error) {
	merged := cp.defOpts
	merged = append(merged, opts...)
	switch typ {
	case TransportHTTP:
		return bigquery.NewProjectRESTClient(ctx, merged...)
	case TransportGRPC:
		return bigquery.NewProjectClient(ctx, merged...)
	default:
		return nil, fmt.Errorf("Invalid transport type: %q", typ)
	}
}

func (cp *ClientProvider) RowAccessPolicyClient(ctx context.Context, typ TransportType, opts ...option.ClientOption) (*bigquery.RowAccessPolicyClient, error) {
	merged := cp.defOpts
	merged = append(merged, opts...)
	switch typ {
	case TransportHTTP:
		return bigquery.NewRowAccessPolicyRESTClient(ctx, merged...)
	case TransportGRPC:
		return bigquery.NewRowAccessPolicyClient(ctx, merged...)
	default:
		return nil, fmt.Errorf("Invalid transport type: %q", typ)
	}
}
