// Copyright 2022 Google LLC
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

package transform

import (
	"fmt"
	"reflect"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type transformer struct {
	descriptor protoreflect.MessageDescriptor
	instance   interface{}
}

func NewTransformer(d protoreflect.MessageDescriptor) (*transformer, error) {
	if d == nil {
		return nil, fmt.Errorf("no descriptor")
	}
	return &transformer{
		descriptor: d,
	}, nil
}

func (t *transformer) Transform(inStruct interface{}) ([]byte, error) {
	vStruct := reflect.ValueOf(inStruct)
	if vStruct.Kind() == reflect.Ptr {
		vStruct = vStruct.Elem()
	}
	if !vStruct.IsValid() {
		return nil, nil
	}
	if vStruct.Kind() != reflect.Struct {
		return nil, fmt.Errorf("provided type is %s, need struct or struct pointer", vStruct.Type())
	}
	return nil, fmt.Errorf("unimplemented")
}

func (t *transformer) createMessage() *dynamicpb.Message {
	return dynamicpb.NewMessage(t.descriptor)
}

func (t *transformer) setInt64Val(m *dynamicpb.Message, fieldName string, val int64) bool {
	fd := m.Descriptor().Fields().ByName(protoreflect.Name(fieldName))
	if fd == nil {
		return false
	}
	m.Set(fd, protoreflect.ValueOfInt64(val))
	return true
}

func (t *transformer) setStringVal(m *dynamicpb.Message, fieldName, val string) bool {
	fd := m.Descriptor().Fields().ByName(protoreflect.Name(fieldName))
	if fd == nil {
		return false
	}
	m.Set(fd, protoreflect.ValueOfString(val))
	return true
}
