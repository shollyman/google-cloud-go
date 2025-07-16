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
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
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

const (
	clientFieldPrefix = "int"
)

func main() {
	flag.Parse()
	log.Printf("enumerating files in %q", *sourceDir)
	files, err := listClientFiles(*sourceDir)
	if err != nil {
		log.Fatalf("listClientFiles: %v", err)
	}

	// contains the inputs from the generated source files.
	sourceFset := token.NewFileSet()
	// keyed by the public RPC name, value is the slice of methods that are RPCs
	rpcMap := make(map[string][]*ast.FuncDecl)

	// contains the output aggregated client.
	destFset := token.NewFileSet()

	for _, f := range files {
		path := filepath.Join(*sourceDir, f)
		log.Printf("processing %q", path)
		if err := collectClientsAndRPCs(sourceFset, rpcMap, path); err != nil {
			log.Fatalf("collectClientsAndRPCs(%q): %v", path, err)
		}
		for typ, funcs := range rpcMap {
			for _, fn := range funcs {
				log.Printf("type %q exposes func %q with calloptions", typ, fn.Name)
			}
		}
	}

	destFile, err := parser.ParseFile(destFset, "client.tmpl", nil, 0)
	if err != nil {
		log.Fatalf("failed to parse output template: %v", err)
	}
	// manipulate the destination AST.
	if err := augmentClientWithFields(destFile, rpcMap); err != nil {
		log.Fatalf("augmentClientWithFields: %v", err)
	}

	// Now, format and print.
	var buf bytes.Buffer
	if err := format.Node(&buf, destFset, destFile); err != nil {
		log.Fatalf("formatting failed: %v", err)
	}
	log.Printf("output source:\n%s", buf.String())

}

// augmentClient modifies the destination client with internal fields
// and adds the methods.
//
// new fields bear the "base" prefix
func augmentClientWithFields(dest *ast.File, rpcMap map[string][]*ast.FuncDecl) error {
	log.Printf("rpcMap has %d clients", len(rpcMap))
	// reference to the aggregate client Type.
	var clientStruct *ast.StructType
	ast.Inspect(dest, func(n ast.Node) bool {
		if gn, ok := n.(*ast.GenDecl); ok {
			if ts, ok := gn.Specs[0].(*ast.TypeSpec); ok {
				log.Printf("found %q", ts.Name.Name)
				if ts.Name.Name == "Client" {
					// Ensure it's also a struct type.
					if stType, ok := ts.Type.(*ast.StructType); ok {
						clientStruct = stType
						return false
					}
				}
			}
		}
		return true
	})
	if clientStruct == nil {
		return fmt.Errorf("couldn't find client type in dest")
	}
	for clientName, rpcs := range rpcMap {
		// construct a new field
		fieldName := fmt.Sprintf("%s%s", clientFieldPrefix, clientName)
		log.Printf("trying to add %q", fieldName)
		newField := &ast.Field{
			Names: []*ast.Ident{ast.NewIdent(fieldName)},
			Type:  ast.NewIdent(fmt.Sprintf("*bigquery.%s", clientName)),
		}
		clientStruct.Fields.List = append(clientStruct.Fields.List, newField)
		// now add the RPCs as methods wired to the internal field.
		recvField := &ast.Field{
			Names: []*ast.Ident{ast.NewIdent("c")},
			Type: &ast.StarExpr{
				X: ast.NewIdent("Client"),
			},
		}
		for _, rpc := range rpcs {
			newFuncDecl := &ast.FuncDecl{
				Name: ast.NewIdent(rpc.Name.Name),
				Recv: &ast.FieldList{
					List: []*ast.Field{
						recvField,
					},
				},
				Type: &ast.FuncType{
					// TODO: params
					// TODO: results
					Results: &ast.FieldList{
						List: []*ast.Field{
							{
								Type: ast.NewIdent("error"),
							},
						},
					},
				},
				Body: &ast.BlockStmt{
					List: []ast.Stmt{
						&ast.ReturnStmt{
							Results: []ast.Expr{
								ast.NewIdent("nil"),
							},
						},
					},
				},
			}
			dest.Decls = append(dest.Decls, newFuncDecl)
		}
	}
	return nil
}

// collectClientsAndRPCs scans a generated source file looking for clients with RPC
// methods.  Our detection heuristic is simple:
//
// * client must be an exported type that ends with "Client" in the name.
// * the last argument to the method must be the variadic gax.CallOption.
func collectClientsAndRPCs(fset *token.FileSet, clientMap map[string][]*ast.FuncDecl, fileName string) error {
	astF, err := parser.ParseFile(fset, fileName, nil, 0)
	if err != nil {
		return fmt.Errorf("parser.ParseFile: %w", err)
	}

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
								log.Printf("collecting client %q having RPC %q", id.Name, fn.Name)
								if sl, ok := clientMap[id.Name]; ok {
									clientMap[id.Name] = append(sl, fn)
								} else {
									clientMap[id.Name] = []*ast.FuncDecl{fn}
								}
							}
						}
					}

				}
			}
		}
		return true // Continue traversal
	})

	return nil
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
