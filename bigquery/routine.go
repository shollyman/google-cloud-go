// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bigquery

import (
	"context"
	"fmt"
	"time"

	bq "google.golang.org/api/bigquery/v2"
)

// Routine represents a reference to a BigQuery Routine.
type Routine struct {
	ProjectID string
	DatasetID string
	RoutineID string

	c *Client
}

// FullyQualifiedName returns an identifer for the routine in project.dataset.routine format.
func (r *Routine) FullyQualifiedName() string {
	return fmt.Sprintf("%s.%s.%s", r.ProjectID, r.DatasetID, r.RoutineID)
}

// Metadata fetches the metadata for a given Routine.
func (r *Routine) Metadata(ctx context.Context) (rm *RoutineMetadata, err error) {
	return nil, fmt.Errorf("unimplemented")
}

// Update updates properties of a Routine.
func (r *Routine) Update(ctx context.Context, upd RoutineMetadataToUpdate, etag string) (rm *RoutineMetadata, err error) {
	return nil, fmt.Errorf("unimplemented")
}

// Delete removes a Routine from a dataset.
func (r *Routine) Delete(ctx context.Context) (err error) {
	return fmt.Errorf("unimplemented")
}

// RoutineMetadata represents details of a given BigQuery Routine.
type RoutineMetadata struct {
	ETag              string
	Type              string // Routine Type
	CreationTime      time.Time
	LastModifiedTime  time.Time
	Language          string
	Arguments         []*RoutineArgument
	ReturnType        string   // lies, this is standardsqltype
	ImportedLibraries []string // javascript-only
	Body              string   // definitionbody
}

// the call arguments for a routine.
type RoutineArgument struct {
	Name     string
	Mode     string
	DataType string // lies, this is standardsqltype
}

// RoutineMetadataToUpdate governs updating a routine.
type RoutineMetadataToUpdate struct {
}

func (rm *RoutineMetadataToUpdate) toBQ() (*bq.Routine, error) {
	return nil, fmt.Errorf("unimplemented")
}

func bqToRoutineMetadata(r *bq.Routine) (*RoutineMetadata, error) {
	return nil, fmt.Errorf("unimplemented")
}
