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

// clientgen is the CLI for generating an aggregate RPC client for bigquery.
package main

import (
	"bytes"
	"flag"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"log"
	"path/filepath"
	"strings"

	"cloud.google.com/go/bigquery/v2/internal/clientgen/astutil"
)

var (
	// directory containing generated client sources.
	sourceDir = flag.String("sourcedir", "/usr/local/google/home/shollyman/gorepos/google-cloud-go/bigquery/v2/apiv2/", "directory containing generated client sources")
	destDir   = flag.String("destdir", "", "output directory for aggregate client")
)

func main() {
	flag.Parse()
	log.Printf("listing files in %q", *sourceDir)
	files, err := listClientFiles(*sourceDir)
	if err != nil {
		log.Fatalf("listClientFiles: %v", err)
	}
	log.Printf("found %d source files", len(files))

	// contains the inputs from the generated source files.
	sourceFset := token.NewFileSet()
	// keyed by the public RPC name, value is the slice of methods that are RPCs
	rpcMap := make(map[string][]*ast.FuncDecl)

	// contains the output aggregated client.
	destFset := token.NewFileSet()

	for _, f := range files {
		path := filepath.Join(*sourceDir, f)
		if err := astutil.CollectClientsAndRPCs(sourceFset, rpcMap, path); err != nil {
			log.Fatalf("CollectClientsAndRPCs(%q): %v", path, err)
		}
	}
	totalRPCs := 0
	for _, funcs := range rpcMap {
		totalRPCs = totalRPCs + len(funcs)
	}
	log.Printf("Collected %d clients and %d total RPCs", len(rpcMap), totalRPCs)

	destFile, err := parser.ParseFile(destFset, "../client.tmpl", nil, parser.ParseComments)
	if err != nil {
		log.Fatalf("failed to parse output template: %v", err)
	}

	// Locate our new Client.
	clientStruct, err := astutil.LocateClientType(destFile)
	if err != nil {
		log.Fatalf("locateClientType: %v", err)
	}
	// add fields to the client.
	if err := astutil.AugmentClientFields(destFile, clientStruct, rpcMap); err != nil {
		log.Fatalf("AugmentClientFields: %v", err)
	}
	// add RPCs to the client.
	if err := astutil.AugmentClientMethods(destFile, clientStruct, rpcMap); err != nil {
		log.Fatalf("AugmentClientMethods: %v", err)
	}
	// Add instantiation/close funcs (NewClient/NewRESTClient)
	if err := astutil.AddCommonFuncs(destFile, rpcMap); err != nil {
		log.Fatalf("AddCommonFuncs: %v", err)
	}

	// TODO: write this to an actual output.  In the interim, just log it.
	var buf bytes.Buffer
	if err := format.Node(&buf, destFset, destFile); err != nil {
		log.Fatalf("formatting failed: %v", err)
	}
	log.Printf("\noutput source:\n\n%s", buf.String())

}

func listClientFiles(sourceDir string) ([]string, error) {
	var files []string

	err := filepath.Walk(sourceDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Don't descend beyond the client dir.ww
		if info.IsDir() && path != sourceDir {
			return filepath.SkipDir
		}
		if strings.HasSuffix(info.Name(), ".go") {
			files = append(files, info.Name())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}
