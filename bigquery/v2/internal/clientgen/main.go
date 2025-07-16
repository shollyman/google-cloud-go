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

// clientgen provides a simple AST-based aggregate client
// code generator.
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"log"
	"path/filepath"
	"strings"
)

var (
	// directory containing generated client sources.
	sourceDir = flag.String("sourcedir", "/usr/local/google/home/shollyman/gorepos/google-cloud-go/bigquery/v2/apiv2/", "directory containing generated client sources")
	destDir   = flag.String("destdir", "", "output directory for aggregate client")
)

func main() {
	flag.Parse()
	log.Printf("enumerating files in %q", *sourceDir)
	files, err := listClientFiles(*sourceDir)
	if err != nil {
		log.Fatalf("listClientFiles: %v", err)
	}

	fset := token.NewFileSet()
	for _, f := range files {
		path := filepath.Join(*sourceDir, f)
		log.Printf("processing %q", path)
		rpcMap, err := getRPCMap(fset, path)
		if err != nil {
			log.Fatalf("getRPCMap(%q): %v", path, err)
		}
		for typ, funcs := range rpcMap {
			for _, fn := range funcs {
				log.Printf("type %q exposes func %q with calloptions", typ, fn.Name)
			}
		}
	}
}

func getRPCMap(fset *token.FileSet, fileName string) (map[string][]*ast.FuncDecl, error) {
	astF, err := parser.ParseFile(fset, fileName, nil, 0)
	if err != nil {
		return nil, fmt.Errorf("parser.ParseFile: %w", err)
	}

	// keyed by exported Client name.
	rpcMap := make(map[string][]*ast.FuncDecl)

	// Walk the parsed file, look for methods with a public client receiver
	ast.Inspect(astF, func(n ast.Node) bool {
		if fn, ok := n.(*ast.FuncDecl); ok {
			isRPCFunc := false
			if fn.Name.IsExported() {
				pLen := len(fn.Type.Params.List)
				if pLen != 3 {
					// Not a function we care about.
					return true
				}
				// our RPC filter:  it's a three arg method and the last param
				// is a variadic ellipsis of gax.CallOption
				lastParam := fn.Type.Params.List[2]
				if paramEllipsis, ok := lastParam.Type.(*ast.Ellipsis); ok {
					if sel, ok := paramEllipsis.Elt.(*ast.SelectorExpr); ok {
						if pId, ok := sel.X.(*ast.Ident); ok {
							if pId.Name == "gax" && sel.Sel.Name == "CallOption" {
								isRPCFunc = true
							}
						}
					}
				}
				// Only capture it if it's a method receiver on a publicly exported type.
				if fn.Recv != nil {
					if recvType, ok := fn.Recv.List[0].Type.(*ast.StarExpr); ok {
						if id, ok := recvType.X.(*ast.Ident); ok {
							if id.IsExported() && isRPCFunc {
								if sl, ok := rpcMap[id.Name]; ok {
									rpcMap[id.Name] = append(sl, fn)
								} else {
									rpcMap[id.Name] = []*ast.FuncDecl{fn}
								}
							}
						}
					}

				}
			}
		}
		return true // Continue traversal
	})
	// validate our expectations are correct re: single exported type with RPCs
	if mapLen := len(rpcMap); mapLen > 1 {
		return nil, fmt.Errorf("validation: expected only a single exported type, found %d", mapLen)
	}
	return rpcMap, nil
}

func listClientFiles(sourceDir string) ([]string, error) {
	var files []string

	err := filepath.Walk(sourceDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if strings.HasSuffix(info.Name(), "_client.go") {
			files = append(files, info.Name())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}
