// Copyright 2021 Google LLC
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

package adapt

import (
	"fmt"
	"strings"

	storagepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1beta2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// bqModeToFieldLabelMap holds mapping from field schema mode to proto label.
// proto3 no longer allows use of REQUIRED labels, so we solve that elsewhere
// and simply use optional.
var bqModeToFieldLabelMap = map[storagepb.TableFieldSchema_Mode]descriptorpb.FieldDescriptorProto_Label{
	storagepb.TableFieldSchema_NULLABLE: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL,
	storagepb.TableFieldSchema_REPEATED: descriptorpb.FieldDescriptorProto_LABEL_REPEATED,
	storagepb.TableFieldSchema_REQUIRED: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL,
}

// Allows conversion between BQ schema type and FieldDescriptorProto's type.
var bqTypeToFieldTypeMap = map[storagepb.TableFieldSchema_Type]descriptorpb.FieldDescriptorProto_Type{
	storagepb.TableFieldSchema_BIGNUMERIC: descriptorpb.FieldDescriptorProto_TYPE_BYTES,
	storagepb.TableFieldSchema_BOOL:       descriptorpb.FieldDescriptorProto_TYPE_BOOL,
	storagepb.TableFieldSchema_BYTES:      descriptorpb.FieldDescriptorProto_TYPE_BYTES,
	storagepb.TableFieldSchema_DATE:       descriptorpb.FieldDescriptorProto_TYPE_INT32,
	storagepb.TableFieldSchema_DATETIME:   descriptorpb.FieldDescriptorProto_TYPE_INT64,
	storagepb.TableFieldSchema_DOUBLE:     descriptorpb.FieldDescriptorProto_TYPE_DOUBLE,
	storagepb.TableFieldSchema_GEOGRAPHY:  descriptorpb.FieldDescriptorProto_TYPE_STRING,
	storagepb.TableFieldSchema_INT64:      descriptorpb.FieldDescriptorProto_TYPE_INT64,
	storagepb.TableFieldSchema_NUMERIC:    descriptorpb.FieldDescriptorProto_TYPE_BYTES,
	storagepb.TableFieldSchema_STRING:     descriptorpb.FieldDescriptorProto_TYPE_STRING,
	storagepb.TableFieldSchema_STRUCT:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE,
	storagepb.TableFieldSchema_TIME:       descriptorpb.FieldDescriptorProto_TYPE_INT64,
	storagepb.TableFieldSchema_TIMESTAMP:  descriptorpb.FieldDescriptorProto_TYPE_INT64,
}

// For TableFieldSchema OPTIONAL mode, we use the wrapper types to allow for the
// proper representation of NULL values, as proto3 semantics would just use default value.
var bqTypeToWrapperMap = map[storagepb.TableFieldSchema_Type]string{
	storagepb.TableFieldSchema_BIGNUMERIC: ".google.protobuf.BytesValue",
	storagepb.TableFieldSchema_BOOL:       ".google.protobuf.BoolValue",
	storagepb.TableFieldSchema_BYTES:      ".google.protobuf.BytesValue",
	storagepb.TableFieldSchema_DATE:       ".google.protobuf.Int32Value",
	storagepb.TableFieldSchema_DATETIME:   ".google.protobuf.Int64Value",
	storagepb.TableFieldSchema_DOUBLE:     ".google.protobuf.DoubleValue",
	storagepb.TableFieldSchema_GEOGRAPHY:  ".google.protobuf.StringValue",
	storagepb.TableFieldSchema_INT64:      ".google.protobuf.StringValue",
	storagepb.TableFieldSchema_NUMERIC:    ".google.protobuf.BytesValue",
	storagepb.TableFieldSchema_STRING:     ".google.protobuf.StringValue",
	storagepb.TableFieldSchema_TIME:       ".google.protobuf.Int64Value",
	storagepb.TableFieldSchema_TIMESTAMP:  ".google.protobuf.Int64Value",
}

// filename used by well known types proto
var wellKnownTypesWrapperName = "google/protobuf/wrappers.proto"

func getFileDescriptorFromDependencyMap(depMap map[string]protoreflect.Descriptor, schema *storagepb.TableSchema) protoreflect.Descriptor {
	if depMap == nil {
		return nil
	}

	// we're comparing based on the leaf fields, but we wrap them in the outer TableSchema message.
	b, err := proto.Marshal(schema)
	if err != nil {
		return nil
	}
	if desc, ok := depMap[string(b)]; ok {
		return desc
	}
	return nil
}

func saveFileDescriptorToDependencyMap(depMap map[string]protoreflect.Descriptor, schema *storagepb.TableSchema, desc protoreflect.Descriptor) error {
	if depMap == nil {
		return fmt.Errorf("dependency map is nil")
	}
	b, err := proto.Marshal(schema)
	if err != nil {
		return fmt.Errorf("failed to serialize tableschema: %v", err)
	}
	depMap[string(b)] = desc
	return nil
}

// StorageSchemaToDescriptor builds a protoreflect.Descriptor for a given table schema.
//
func StorageSchemaToDescriptor(inSchema *storagepb.TableSchema, scope string, dependencyMap map[string]protoreflect.Descriptor) (protoreflect.Descriptor, error) {
	if inSchema == nil {
		return nil, fmt.Errorf("no input schema provided")
	}
	/*
		if dependencyMap == nil {
			dependencyMap = make(map[*storagepb.TableSchema]protoreflect.Descriptor)
		}
	*/

	var fields []*descriptorpb.FieldDescriptorProto
	var deps []protoreflect.FileDescriptor
	var fNumber int32

	for _, f := range inSchema.GetFields() {
		fNumber = fNumber + 1
		currentScope := fmt.Sprintf("%s__%s", scope, f.GetName())
		// If we're dealing with a STRUCT type, we must deal with sub messages.
		// As multiple submessages may share the same type definition, we use a dependency cache
		// and interrogate it / populate it as we're going.
		if f.Type == storagepb.TableFieldSchema_STRUCT {
			foundDesc := getFileDescriptorFromDependencyMap(dependencyMap, &storagepb.TableSchema{Fields: f.GetFields()})
			if foundDesc != nil {
				// check to see if we already have this in current dependency list
				haveDep := false
				for _, curDep := range deps {
					if foundDesc.ParentFile().FullName() == curDep.FullName() {
						haveDep = true
						break
					}
				}
				// if dep is missing, add to current dependencies
				if !haveDep {
					deps = append(deps, foundDesc.ParentFile())
				}
				// construct field descriptor for the message
				fdp, err := tableFieldSchemaToFieldDescriptorProto(f, fNumber, string(foundDesc.FullName()))
				if err != nil {
					return nil, fmt.Errorf("couldn't convert to FieldDescriptorProto (%s): %v", currentScope, err)
				}
				fields = append(fields, fdp)
			} else {
				// Wrap the current struct's fields in a TableSchema outer message, and then build the submessage.
				ts := &storagepb.TableSchema{
					Fields: f.GetFields(),
				}
				desc, err := StorageSchemaToDescriptor(ts, currentScope, dependencyMap)
				if err != nil {
					return nil, fmt.Errorf("couldn't compile (%s): %v", currentScope, err)
				}
				// Now that we have the submessage definition, we append it both to the local dependencies, as well
				// as inserting it into the depMap cache for possible reuse elsewhere.
				deps = append(deps, desc.ParentFile())
				saveFileDescriptorToDependencyMap(dependencyMap, ts, desc)

				fdp, err := tableFieldSchemaToFieldDescriptorProto(f, fNumber, currentScope)
				if err != nil {
					return nil, fmt.Errorf("couldn't compute field schema (%s): %v", currentScope, err)
				}
				fields = append(fields, fdp)
			}
		} else {
			fd, err := tableFieldSchemaToFieldDescriptorProto(f, fNumber, currentScope)
			if err != nil {
				return nil, err
			}
			fields = append(fields, fd)
		}
	}
	// Start constructing a DescriptorProto.
	dp := &descriptorpb.DescriptorProto{
		Name:  proto.String(scope),
		Field: fields,
	}

	// Use the local dependencies to generate a list of filenames.
	depNames := []string{
		wellKnownTypesWrapperName,
	}
	for _, d := range deps {
		depNames = append(depNames, d.ParentFile().Path())
	}

	// Now, construct a FileDescriptorProto.
	fdp := &descriptorpb.FileDescriptorProto{
		MessageType: []*descriptorpb.DescriptorProto{dp},
		Name:        proto.String(fmt.Sprintf("%s.proto", scope)),
		Syntax:      proto.String("proto3"),
		Dependency:  depNames,
	}

	// We'll need a FileDescriptorSet as we have a FileDescriptorProto for the current
	// descriptor we're building, but we need to include all the referenced dependencies.
	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			fdp,
			protodesc.ToFileDescriptorProto(wrapperspb.File_google_protobuf_wrappers_proto),
		},
	}
	for _, d := range deps {
		fds.File = append(fds.File, protodesc.ToFileDescriptorProto(d))
	}

	// Load the set into a registy, then interrogate it for the descriptor corresponding to the top level message.
	files, err := protodesc.NewFiles(fds)
	if err != nil {
		return nil, fmt.Errorf("protodesc.NewFiles: %v", err)
	}
	return files.FindDescriptorByName(protoreflect.FullName(scope))
}

// tableFieldSchemaToFieldDescriptorProto builds individual field descriptors for a proto message.
// We're using proto3 syntax, but BigQuery supports the notion of NULLs which conflicts with proto3 default value
// behavior.  To enable it, we look for nullable fields in the schema that should be scalars, and use the
// well-known wrapper types.
//
// Messages are always nullable, and repeated fields are as well.
func tableFieldSchemaToFieldDescriptorProto(field *storagepb.TableFieldSchema, idx int32, scope string) (*descriptorpb.FieldDescriptorProto, error) {
	name := strings.ToLower(field.GetName())
	if field.GetType() == storagepb.TableFieldSchema_STRUCT {
		return &descriptorpb.FieldDescriptorProto{
			Name:     proto.String(name),
			Number:   proto.Int32(idx),
			TypeName: proto.String(scope),
			Label:    bqModeToFieldLabelMap[field.GetMode()].Enum(),
		}, nil
	}

	// For non-NULLABLE fields, we use the expected scalar types, but the proto is
	// still marked OPTIONAL (proto3 semantics).
	if field.GetMode() != storagepb.TableFieldSchema_NULLABLE {
		return &descriptorpb.FieldDescriptorProto{
			Name:   proto.String(name),
			Number: proto.Int32(idx),
			Type:   bqTypeToFieldTypeMap[field.GetType()].Enum(),
			Label:  bqModeToFieldLabelMap[field.GetMode()].Enum(),
		}, nil
	}
	// For NULLABLE, we use the wrapper types.
	return &descriptorpb.FieldDescriptorProto{
		Name:     proto.String(name),
		Number:   proto.Int32(idx),
		Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
		TypeName: proto.String(bqTypeToWrapperMap[field.GetType()]),
		Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
	}, nil
}
