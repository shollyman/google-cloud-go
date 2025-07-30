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
	"maps"
	"slices"
	"strings"

	"golang.org/x/tools/go/ast/astutil"
)

const (
	clientFieldPrefix = "int"
)

// SetupOutputFile handles the initial setup of the output file.
// It defines the new ast.File, sets up package name, imports, and stubs
// both the Client type and the necessary exported functions.
func SetupOutputFile(fset *token.FileSet, pkgName string) (*ast.File, error) {
	f, err := parser.ParseFile(fset, "client.go", fmt.Sprintf("package %s", pkgName), 0)
	if err != nil {
		return nil, fmt.Errorf("ParseFile: %w", err)
	}
	f.Name = ast.NewIdent(pkgName)
	astutil.AddImport(fset, f, "fmt")
	astutil.AddImport(fset, f, "context")
	astutil.AddImport(fset, f, "google.golang.org/api/option")
	astutil.AddNamedImport(fset, f, "gax", "github.com/googleapis/gax-go/v2")
	astutil.AddNamedImport(fset, f, "bigquery", "cloud.google.com/go/bigquery/v2/apiv2")
	astutil.AddImport(fset, f, "cloud.google.com/go/bigquery/v2/apiv2/bigquerypb")
	// Declare our "Client" struct type.
	f.Decls = append(f.Decls, &ast.GenDecl{
		Tok: token.TYPE,
		Specs: []ast.Spec{
			&ast.TypeSpec{
				Name: ast.NewIdent("Client"),
				Type: &ast.StructType{
					Fields: &ast.FieldList{},
				},
			},
		},
	})
	// Declare our three exported funcs: NewClient, NewRESTClient, Close.
	f.Decls = append(f.Decls, &ast.FuncDecl{
		Name: ast.NewIdent("NewClient"),
		Type: &ast.FuncType{
			Params: &ast.FieldList{
				List: []*ast.Field{
					{
						Names: []*ast.Ident{ast.NewIdent("ctx")},
						Type: &ast.SelectorExpr{
							X:   ast.NewIdent("context"),
							Sel: ast.NewIdent("Context"),
						},
					},
					{
						Names: []*ast.Ident{ast.NewIdent("opts")},
						Type: &ast.Ellipsis{
							Elt: &ast.SelectorExpr{
								X:   ast.NewIdent("option"),
								Sel: ast.NewIdent("ClientOption"),
							},
						},
					},
				},
			},
			Results: &ast.FieldList{
				List: []*ast.Field{
					{
						Type: &ast.StarExpr{
							X: ast.NewIdent("Client"),
						},
					},
					{
						Type: ast.NewIdent("error"),
					},
				},
			},
		},
	})
	f.Decls = append(f.Decls, &ast.FuncDecl{
		Name: ast.NewIdent("NewRESTClient"),
		Type: &ast.FuncType{
			Params: &ast.FieldList{
				List: []*ast.Field{
					{
						Names: []*ast.Ident{ast.NewIdent("ctx")},
						Type: &ast.SelectorExpr{
							X:   ast.NewIdent("context"),
							Sel: ast.NewIdent("Context"),
						},
					},
					{
						Names: []*ast.Ident{ast.NewIdent("opts")},
						Type: &ast.Ellipsis{
							Elt: &ast.SelectorExpr{
								X:   ast.NewIdent("option"),
								Sel: ast.NewIdent("ClientOption"),
							},
						},
					},
				},
			},
			Results: &ast.FieldList{
				List: []*ast.Field{
					{
						Type: &ast.StarExpr{
							X: ast.NewIdent("Client"),
						},
					},
					{
						Type: ast.NewIdent("error"),
					},
				},
			},
		},
	})
	f.Decls = append(f.Decls, &ast.FuncDecl{
		Name: ast.NewIdent("Close"),
		Type: &ast.FuncType{
			Results: &ast.FieldList{
				List: []*ast.Field{
					{
						Type: ast.NewIdent("error"),
					},
				},
			},
		},
	})

	return f, nil

}

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
			if len(gn.Specs) == 1 {
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
	// traverse the map in order to avoid jitter in the output.
	for _, clientName := range slices.Sorted(maps.Keys(rpcMap)) {
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
	for _, clientName := range slices.Sorted(maps.Keys(rpcMap)) {
		rpcSlice := rpcMap[clientName]
		slices.SortFunc(rpcSlice, func(a, b *ast.FuncDecl) int {
			return strings.Compare(a.Name.Name, b.Name.Name)
		})
		for _, rpc := range rpcSlice {
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
				Body: buildRPCReturnBlock(clientName, rpc),
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
func AddCommonFuncs(dest *ast.File, rpcMap map[string][]*ast.FuncDecl) error {

	clientSlice := slices.Sorted(maps.Keys(rpcMap))

	// NewClient
	fn, err := locateClientFunc(dest, "NewClient")
	if err != nil {
		return err
	}
	fn.Body = addCreationFuncBlock(true, clientSlice)
	// NewRESTClient
	fn, err = locateClientFunc(dest, "NewRESTClient")
	if err != nil {
		return err
	}
	fn.Body = addCreationFuncBlock(false, clientSlice)

	// Close
	fn, err = locateClientFunc(dest, "Close")
	if err != nil {
		return err
	}
	fn.Body = genCloseFuncBlock(clientSlice)

	return nil
}

func locateClientFunc(dest *ast.File, funcName string) (*ast.FuncDecl, error) {
	var fn *ast.FuncDecl
	ast.Inspect(dest, func(n ast.Node) bool {
		if foundFn, ok := n.(*ast.FuncDecl); ok {
			if foundFn.Name.Name == funcName {
				fn = foundFn
				return false
			}
		}
		return true
	})
	if fn == nil {
		return nil, fmt.Errorf("couldn't find function %q in destination", funcName)
	}
	return fn, nil
}

func addCreationFuncBlock(isGRPC bool, clientNames []string) *ast.BlockStmt {
	stmts := []ast.Stmt{
		&ast.DeclStmt{
			Decl: &ast.GenDecl{
				Tok: token.VAR,
				Specs: []ast.Spec{
					&ast.ValueSpec{
						Names: []*ast.Ident{
							ast.NewIdent("errs"),
						},
						Type: &ast.ArrayType{
							Elt: ast.NewIdent("error"),
						},
					},
				},
			},
		},
		&ast.DeclStmt{
			Decl: &ast.GenDecl{
				Tok: token.VAR,
				Specs: []ast.Spec{
					&ast.ValueSpec{
						Names: []*ast.Ident{
							ast.NewIdent("err"),
						},
						Type: ast.NewIdent("error"),
					},
				},
			},
		},
		&ast.AssignStmt{
			Tok: token.DEFINE,
			Lhs: []ast.Expr{
				ast.NewIdent("c"),
			},
			Rhs: []ast.Expr{
				&ast.UnaryExpr{
					Op: token.AND,
					X: &ast.CompositeLit{
						Type: ast.NewIdent("Client"),
					},
				},
			},
		},
	}

	for _, client := range clientNames {
		targetClient := fmt.Sprintf("%s%s", clientFieldPrefix, client)
		targetNewFnName := fmt.Sprintf("New%s", client)
		if !isGRPC {
			targetNewFnName = strings.Replace(targetNewFnName, "Client", "RESTClient", 1)
		}
		stmts = append(stmts,
			&ast.AssignStmt{
				Tok: token.ASSIGN,
				Lhs: []ast.Expr{
					&ast.SelectorExpr{
						X:   ast.NewIdent("c"),
						Sel: ast.NewIdent(targetClient),
					},
					ast.NewIdent("err"),
				},
				Rhs: []ast.Expr{
					&ast.CallExpr{
						Fun: &ast.SelectorExpr{
							X:   ast.NewIdent("bigquery"),
							Sel: ast.NewIdent(targetNewFnName),
						},
						Args: []ast.Expr{
							ast.NewIdent("ctx"),
							ast.NewIdent("opts"),
						},
					},
				},
			})
		stmts = append(stmts, &ast.IfStmt{
			Cond: &ast.BinaryExpr{
				Op: token.NEQ,
				X:  ast.NewIdent("err"),
				Y:  ast.NewIdent("nil"),
			},
			Body: &ast.BlockStmt{
				List: []ast.Stmt{
					&ast.AssignStmt{
						Tok: token.ASSIGN,
						Lhs: []ast.Expr{
							ast.NewIdent("errs"),
						},
						Rhs: []ast.Expr{
							&ast.CallExpr{
								Fun: ast.NewIdent("append"),
								Args: []ast.Expr{
									ast.NewIdent("errs"),
									&ast.CallExpr{
										Fun: &ast.SelectorExpr{
											X:   ast.NewIdent("fmt"),
											Sel: ast.NewIdent("Errorf"),
										},
										Args: []ast.Expr{
											&ast.BasicLit{
												Kind:  token.STRING,
												Value: fmt.Sprintf("\"%s: %%w\"", targetNewFnName),
											},
											ast.NewIdent("err"),
										},
									},
								},
							},
						},
					},
				},
			},
		})
	}

	// Final error checks.
	stmts = append(stmts, &ast.IfStmt{
		Cond: &ast.BinaryExpr{
			Op: token.GTR,
			X: &ast.CallExpr{
				Fun: ast.NewIdent("len"),
				Args: []ast.Expr{
					ast.NewIdent("errs"),
				},
			},
			Y: &ast.BasicLit{
				Kind:  token.INT,
				Value: "0",
			},
		},
		Body: &ast.BlockStmt{
			List: []ast.Stmt{
				&ast.ReturnStmt{
					Results: []ast.Expr{
						ast.NewIdent("nil"),
						&ast.CallExpr{
							Fun: &ast.SelectorExpr{
								X:   ast.NewIdent("errors"),
								Sel: ast.NewIdent("Join"),
							},
							Args: []ast.Expr{
								ast.NewIdent("errs"),
							},
						},
					},
				},
			},
		},
	})
	stmts = append(stmts, &ast.ReturnStmt{
		Results: []ast.Expr{
			ast.NewIdent("c"),
			ast.NewIdent("nil"),
		},
	})
	return &ast.BlockStmt{
		List: stmts,
	}
}

func genCloseFuncBlock(clientNames []string) *ast.BlockStmt {
	stmts := []ast.Stmt{
		&ast.DeclStmt{
			Decl: &ast.GenDecl{
				Tok: token.VAR,
				Specs: []ast.Spec{
					&ast.ValueSpec{
						Names: []*ast.Ident{
							ast.NewIdent("errs"),
						},
						Type: &ast.ArrayType{
							Elt: ast.NewIdent("error"),
						},
					},
				},
			},
		},

		&ast.DeclStmt{
			Decl: &ast.GenDecl{
				Tok: token.VAR,
				Specs: []ast.Spec{
					&ast.ValueSpec{
						Names: []*ast.Ident{
							ast.NewIdent("errs"),
						},
						Type: ast.NewIdent("error"),
					},
				},
			},
		},
	}

	for _, client := range clientNames {
		targetClient := fmt.Sprintf("%s%s", clientFieldPrefix, client)

		stmts = append(stmts,
			&ast.AssignStmt{
				Tok: token.ASSIGN,
				Lhs: []ast.Expr{
					ast.NewIdent("err"),
				},
				Rhs: []ast.Expr{
					&ast.CallExpr{
						Fun: &ast.SelectorExpr{
							X: &ast.SelectorExpr{
								X:   ast.NewIdent("c"),
								Sel: ast.NewIdent(targetClient),
							},
							Sel: ast.NewIdent("Close"),
						},
					},
				},
			})
		stmts = append(stmts, &ast.IfStmt{
			Cond: &ast.BinaryExpr{
				Op: token.NEQ,
				X:  ast.NewIdent("err"),
				Y:  ast.NewIdent("nil"),
			},
			Body: &ast.BlockStmt{
				List: []ast.Stmt{
					&ast.AssignStmt{
						Tok: token.ASSIGN,
						Lhs: []ast.Expr{
							ast.NewIdent("errs"),
						},
						Rhs: []ast.Expr{
							&ast.CallExpr{
								Fun: ast.NewIdent("append"),
								Args: []ast.Expr{
									ast.NewIdent("errs"),
									&ast.CallExpr{
										Fun: &ast.SelectorExpr{
											X:   ast.NewIdent("fmt"),
											Sel: ast.NewIdent("Errorf"),
										},
										Args: []ast.Expr{
											&ast.BasicLit{
												Kind:  token.STRING,
												Value: fmt.Sprintf("\"%s.Close(): %%w\"", client),
											},
											ast.NewIdent("err"),
										},
									},
								},
							},
						},
					},
				},
			},
		})
	}

	// Final error checks.
	stmts = append(stmts, &ast.IfStmt{
		Cond: &ast.BinaryExpr{
			Op: token.GTR,
			X: &ast.CallExpr{
				Fun: ast.NewIdent("len"),
				Args: []ast.Expr{
					ast.NewIdent("errs"),
				},
			},
			Y: &ast.BasicLit{
				Kind:  token.INT,
				Value: "0",
			},
		},
		Body: &ast.BlockStmt{
			List: []ast.Stmt{
				&ast.ReturnStmt{
					Results: []ast.Expr{
						&ast.CallExpr{
							Fun: &ast.SelectorExpr{
								X:   ast.NewIdent("errors"),
								Sel: ast.NewIdent("Join"),
							},
							Args: []ast.Expr{
								ast.NewIdent("errs"),
							},
						},
					},
				},
			},
		},
	})
	stmts = append(stmts, &ast.ReturnStmt{
		Results: []ast.Expr{
			ast.NewIdent("nil"),
		},
	})
	return &ast.BlockStmt{
		List: stmts,
	}
}
