// Copyright 2020 Google LLC
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

package storage // import "cloud.google.com/go/bigquery/storage/apiv1alpha2"

import (
	"cloud.google.com/go/bigquery"
	storagepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1alpha2"
)

var fieldTypeMap = map[bigquery.FieldType]storagepb.TableFieldSchema_Type{
	bigquery.StringFieldType:    storagepb.TableFieldSchema_STRING,
	bigquery.BytesFieldType:     storagepb.TableFieldSchema_BYTES,
	bigquery.IntegerFieldType:   storagepb.TableFieldSchema_INT64,
	bigquery.FloatFieldType:     storagepb.TableFieldSchema_DOUBLE,
	bigquery.BooleanFieldType:   storagepb.TableFieldSchema_BOOL,
	bigquery.TimestampFieldType: storagepb.TableFieldSchema_TIMESTAMP,
	bigquery.RecordFieldType:    storagepb.TableFieldSchema_STRUCT,
	bigquery.DateFieldType:      storagepb.TableFieldSchema_DATE,
	bigquery.TimeFieldType:      storagepb.TableFieldSchema_TIME,
	bigquery.DateTimeFieldType:  storagepb.TableFieldSchema_DATETIME,
	bigquery.NumericFieldType:   storagepb.TableFieldSchema_NUMERIC,
	bigquery.GeographyFieldType: storagepb.TableFieldSchema_GEOGRAPHY,
}

func bqTypeToProtoType(in bigquery.FieldType) storagepb.TableFieldSchema_Type {
	if val, ok := fieldTypeMap[in]; ok {
		return val
	}
	return storagepb.TableFieldSchema_TYPE_UNSPECIFIED
}

func convertToProtoMode(in *bigquery.FieldSchema) storagepb.TableFieldSchema_Mode {
	if in.Repeated {
		return storagepb.TableFieldSchema_REPEATED
	}
	if in.Required {
		return storagepb.TableFieldSchema_REQUIRED
	}
	return storagepb.TableFieldSchema_NULLABLE
}

func BigQueryToProtoSchema(in bigquery.Schema) *storagepb.TableSchema {
	if in == nil {
		return nil
	}
	fields := bqToTableField(in)

	return &storagepb.TableSchema{
		Fields: fields,
	}
}

func bqToTableField(in bigquery.Schema) []*storagepb.TableFieldSchema {
	if in == nil {
		return nil
	}
	var out []*storagepb.TableFieldSchema
	for _, v := range in {
		s := &storagepb.TableFieldSchema{
			Type:        bqTypeToProtoType(v.Type),
			Mode:        convertToProtoMode(v),
			Description: v.Description,
			Name:        v.Name,
		}
		if s.Type == storagepb.TableFieldSchema_STRUCT {
			s.Fields = bqToTableField(v.Schema)
		}
		out = append(out, s)
	}
	return out
}
