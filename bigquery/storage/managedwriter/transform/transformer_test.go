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
	"bytes"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestTransformer(t *testing.T) {
	testCases := []struct {
		desc             string
		msgDescriptor    protoreflect.MessageDescriptor
		instance         interface{}
		wantCreationErr  bool
		wantTransformErr bool
		wantBytes        []byte
	}{
		{
			desc:            "nil descriptor",
			instance:        nil,
			wantCreationErr: true,
		},
	}

	for _, tc := range testCases {
		trans, err := NewTransformer(tc.msgDescriptor)
		if err != nil {
			if !tc.wantCreationErr {
				t.Errorf("%s: failed to instantiate transformer: %v", tc.desc, err)
			}
		}
		if err == nil && tc.wantCreationErr {
			t.Errorf("%s: wanted error creating transformer, but succeeded", tc.desc)
		}

		msgBytes, err := trans.Transform(tc.instance)
		if err != nil {
			t.Errorf("%s failed Transform: %v", tc.desc, err)
		}

		if !bytes.Equal(msgBytes, tc.wantBytes) {
			t.Errorf("%s: bytes mismatch", tc.desc)
		}
	}
}
