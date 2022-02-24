// Copyright 2015 Google LLC
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
	"testing"
)

func TestRepro_FlateErrors(t *testing.T) {
	// Attempts to repro https://github.com/googleapis/google-cloud-go/issues/5478
	numRuns := 100
	if client == nil {
		t.Skip("Repro test skipped")
	}
	ctx := context.Background()

	sql := `
	SELECT vals.c_event_name FROM UNNEST([
		STRUCT("purchase" as c_event_name,"2022-01-28T08:16:01.139Z" as c_created_at,"a_user1" as c_cdp_user_id,"55555" as c_client_id,"user1"),
		STRUCT("page_view","2022-01-28T08:16:01.139Z","a_user2","111111","user2"),
		STRUCT("page_view","2022-01-28T08:16:01.139Z","a_user2","111111","user2"),
		STRUCT("事件","2021-09-22T07:00:00Z","測試","APPu1","user1"),
		STRUCT("事件","2021-09-22T07:00:00Z","測試","APPu1","user1")
	]) as vals ORDER BY RAND()`
	q := client.Query(sql)
	q.DisableQueryCache = true

	for i := 0; i < numRuns; i++ {

		job, err := q.Run(ctx)
		if err != nil {
			t.Errorf("Run %d failed: %v", i, err)
		}
		it, err := job.Read(ctx)
		if err != nil {
			t.Errorf("Read %d failed (%q): %v", i, job.jobID, err)
		}

		// read a row, to ensure we're populating the iterator page.
		var row []Value
		err = it.Next(&row)
		if err != nil {
			t.Errorf("Next %d failed (%q): %v", i, job.jobID, err)
		}
		t.Logf("#%d (%q) row: %v", i, job.jobID, row)

	}
}
