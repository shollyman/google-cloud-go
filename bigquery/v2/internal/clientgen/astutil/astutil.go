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

// Package astutil contains the AST manipulation functionality for building
// an aggregate client for the bigquery v2 API surfaces.
package astutil

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
)

const (
	clientFieldPrefix = "int"
)

// CollectClientRPCs is used to find the set of exported clients and their associated
// RPCs.  It records them in the provided clientMap, keying the public Client name to
// the slice of FuncDecls that represent the RPC method receivers.
//
// Our main criteria for finding RPCs
// - the function is exported and has a method receiver
// - the method receiver is exported and ends in the string "Client"
// - the last argument of the function params is a variadic gax.CallOption
func CollectClientsAndRPCs(fset *token.FileSet, clientMap map[string][]*ast.FuncDecl, fileName string) error {
	astF, err := parser.ParseFile(fset, fileName, nil, parser.ParseComments)
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
								if strings.HasSuffix(id.Name, "Client") {
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
		}
		return true // Continue traversal
	})

	return nil
}

// LocateClientType is used to find the struct node corresponding to the
// new aggregate client in the parsed output template.
func LocateClientType(dest *ast.File) (*ast.StructType, error) {
	var clientStruct *ast.StructType
	ast.Inspect(dest, func(n ast.Node) bool {
		if gn, ok := n.(*ast.GenDecl); ok {
			if ts, ok := gn.Specs[0].(*ast.TypeSpec); ok {
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
		return nil, fmt.Errorf("couldn't find client type in destination")
	}
	return clientStruct, nil
}

// NormalizeRPCParams normalizes the input params to our generated RPC functions
// in the output client.
func NormalizeRPCParams(in *ast.FieldList) *ast.FieldList {
	// TODO: is there anything we need to clean up here?
	return in
}

// NormalizeRPCResults normalizes the output results for the generated RPC
// functions in the output client.
//
// Our main goal here is to normalize any references that are a StarExpr
// (appropriate for a reference in the same package) to a
// SelectorExpr since the aggregate client lives in a different package.
// The main use case here is the list iterator types.
func NormalizeRPCResults(in *ast.FieldList) *ast.FieldList {
	out := &ast.FieldList{}
	for _, inField := range in.List {
		if star, ok := inField.Type.(*ast.StarExpr); ok {
			if id, ok := star.X.(*ast.Ident); ok {
				// refactor to selectorExpr
				newSelector := &ast.SelectorExpr{
					X:   ast.NewIdent("bigquery"),
					Sel: ast.NewIdent(id.Name),
				}
				star.X = newSelector
			}
		}
		out.List = append(out.List, inField)
	}
	return out
}

// AugmentClientFields manipulates the AST to decorate the aggregate client with the member fields
// that hold the base clients.
func AugmentClientFields(dest *ast.File, clientStruct *ast.StructType, rpcMap map[string][]*ast.FuncDecl) error {
	if len(rpcMap) == 0 {
		return fmt.Errorf("no entries present in the rpcMap")
	}
	for clientName, _ := range rpcMap {
		// construct a new field
		fieldName := fmt.Sprintf("%s%s", clientFieldPrefix, clientName)
		newField := &ast.Field{
			Names: []*ast.Ident{ast.NewIdent(fieldName)},
			Type:  ast.NewIdent(fmt.Sprintf("*bigquery.%s", clientName)),
		}
		clientStruct.Fields.List = append(clientStruct.Fields.List, newField)
	}
	return nil
}

// AugmentClientMethods adds the proxied RPC FuncDecls to the aggregate client type.
func AugmentClientMethods(dest *ast.File, clientStruct *ast.StructType, rpcMap map[string][]*ast.FuncDecl) error {
	for clientname, rpcs := range rpcMap {
		for _, rpc := range rpcs {
			newFuncDecl := &ast.FuncDecl{
				Name: ast.NewIdent(rpc.Name.Name),
				Recv: &ast.FieldList{
					List: []*ast.Field{
						{
							Names: []*ast.Ident{ast.NewIdent("c")},
							Type: &ast.StarExpr{
								X: ast.NewIdent("Client"),
							},
						},
					},
				},
				Type: &ast.FuncType{
					Params:  NormalizeRPCParams(rpc.Type.Params),
					Results: NormalizeRPCResults(rpc.Type.Results),
				},
				Body: buildRPCReturnBlock(clientname, rpc),
				// TODO: normalize docstring for the new function.
			}
			dest.Decls = append(dest.Decls, newFuncDecl)
		}
	}
	return nil
}

// buildMethodReturnBlock populates the RPC func impl in the generated client.  Its a
// single return statement of the form: return c.<memberclientField>.<RPCName>(arg names)
func buildRPCReturnBlock(clientName string, rpc *ast.FuncDecl) *ast.BlockStmt {
	var args []ast.Expr
	for _, arg := range rpc.Type.Params.List {
		args = append(args, ast.NewIdent(arg.Names[0].Name))
	}
	block := &ast.BlockStmt{
		List: []ast.Stmt{
			&ast.ReturnStmt{
				Results: []ast.Expr{
					&ast.CallExpr{
						Fun: &ast.SelectorExpr{
							X: &ast.SelectorExpr{
								X:   ast.NewIdent("c"),
								Sel: ast.NewIdent(fmt.Sprintf("%s%s", clientFieldPrefix, clientName)),
							},
							Sel: ast.NewIdent(rpc.Name.Name),
						},
						Args: args,
					},
				},
			},
		},
	}
	return block
}
func addCommonFuncs(dest *ast.File, clientStruct *ast.StructType, rpcMap map[string][]*ast.FuncDecl) error {
	// TODO: NewClient, NewRESTClient, Close, setGoogleClientInfo
	return nil
}
