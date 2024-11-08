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
package previewtesting

import (
	"fmt"
	"log"
	"os"
	"testing"

	"cloud.google.com/go/bigquery/clientprovider"
	"google.golang.org/api/option"
)

var TestClientProvider *clientprovider.ClientProvider
var TestProjectID string

func getProjectID() string {
	if p, ok := os.LookupEnv("GCLOUD_TESTS_GOLANG_PROJECT_ID"); ok {
		return p
	}
	return "NO_PROJECT_ID"
}

func TestMain(m *testing.M) {
	err := setup()
	if err != nil {
		log.Printf("failure setting up test environment, skipping test execution: %v", err)
		os.Exit(1)
	}
	code := m.Run()
	shutdown()
	os.Exit(code)
}

func setup() error {
	TestProjectID = getProjectID()
	// TODO: ditch the endpoint override after grpc is lit up fully
	cp, err := clientprovider.NewClientProvider(option.WithEndpoint("test-bigqueryreservation.sandbox.googleapis.com:443"))
	if err != nil {
		return fmt.Errorf("NewClientProvider: %w", err)
	}
	TestClientProvider = cp
	return nil
}

func shutdown() {
	// TODO
}
