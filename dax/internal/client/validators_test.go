/*
  Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License.
  A copy of the License is located at

      http://www.apache.org/licenses/LICENSE-2.0

  or in the "license" file accompanying this file. This file is distributed
  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  express or implied. See the License for the specific language governing
  permissions and limitations under the License.
*/

package client

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
)

// Helper function for creating attribute values
func stringAttr(s string) types.AttributeValue {
	return &types.AttributeValueMemberS{Value: s}
}

func TestValidateConditionCheck(t *testing.T) {
	tests := []struct {
		name    string
		input   *types.ConditionCheck
		wantErr bool
	}{
		{
			name:    "nil input",
			input:   nil,
			wantErr: false,
		},
		{
			name:    "empty input",
			input:   &types.ConditionCheck{},
			wantErr: true,
		},
		{
			name: "missing TableName",
			input: &types.ConditionCheck{
				Key:                 map[string]types.AttributeValue{"id": stringAttr("1")},
				ConditionExpression: aws.String("attribute_exists(id)"),
			},
			wantErr: true,
		},
		{
			name: "valid input",
			input: &types.ConditionCheck{
				TableName:           aws.String("TestTable"),
				Key:                 map[string]types.AttributeValue{"id": stringAttr("1")},
				ConditionExpression: aws.String("attribute_exists(id)"),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateConditionCheck(tt.input)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestValidatePut(t *testing.T) {
	tests := []struct {
		name    string
		input   *types.Put
		wantErr bool
	}{
		{
			name:    "nil input",
			input:   nil,
			wantErr: false,
		},
		{
			name:    "empty input",
			input:   &types.Put{},
			wantErr: true,
		},
		{
			name: "missing Item",
			input: &types.Put{
				TableName: aws.String("TestTable"),
			},
			wantErr: true,
		},
		{
			name: "valid input",
			input: &types.Put{
				TableName: aws.String("TestTable"),
				Item:      map[string]types.AttributeValue{"id": stringAttr("1")},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePut(tt.input)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestValidateDelete(t *testing.T) {
	tests := []struct {
		name    string
		input   *types.Delete
		wantErr bool
	}{
		{
			name:    "nil input",
			input:   nil,
			wantErr: false,
		},
		{
			name:    "empty input",
			input:   &types.Delete{},
			wantErr: true,
		},
		{
			name: "valid input",
			input: &types.Delete{
				TableName: aws.String("TestTable"),
				Key:       map[string]types.AttributeValue{"id": stringAttr("1")},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDelete(tt.input)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestValidateUpdate(t *testing.T) {
	tests := []struct {
		name    string
		input   *types.Update
		wantErr bool
	}{
		{
			name:    "nil input",
			input:   nil,
			wantErr: false,
		},
		{
			name:    "empty input",
			input:   &types.Update{},
			wantErr: true,
		},
		{
			name: "valid input",
			input: &types.Update{
				TableName:        aws.String("TestTable"),
				Key:              map[string]types.AttributeValue{"id": stringAttr("1")},
				UpdateExpression: aws.String("SET #name = :name"),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateUpdate(tt.input)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestValidateTransactWriteItem(t *testing.T) {
	validPut := &types.Put{
		TableName: aws.String("TestTable"),
		Item:      map[string]types.AttributeValue{"id": stringAttr("1")},
	}
	validDelete := &types.Delete{
		TableName: aws.String("TestTable"),
		Key:       map[string]types.AttributeValue{"id": stringAttr("1")},
	}
	validUpdate := &types.Update{
		TableName:        aws.String("TestTable"),
		Key:              map[string]types.AttributeValue{"id": stringAttr("1")},
		UpdateExpression: aws.String("SET #name = :name"),
	}
	validConditionCheck := &types.ConditionCheck{
		TableName:           aws.String("TestTable"),
		Key:                 map[string]types.AttributeValue{"id": stringAttr("1")},
		ConditionExpression: aws.String("attribute_exists(id)"),
	}

	tests := []struct {
		name    string
		input   *types.TransactWriteItem
		wantErr bool
	}{
		{
			name:    "nil input",
			input:   nil,
			wantErr: false,
		},
		{
			name:    "empty input",
			input:   &types.TransactWriteItem{},
			wantErr: false,
		},
		{
			name: "valid Put",
			input: &types.TransactWriteItem{
				Put: validPut,
			},
			wantErr: false,
		},
		{
			name: "valid Delete",
			input: &types.TransactWriteItem{
				Delete: validDelete,
			},
			wantErr: false,
		},
		{
			name: "valid Update",
			input: &types.TransactWriteItem{
				Update: validUpdate,
			},
			wantErr: false,
		},
		{
			name: "valid ConditionCheck",
			input: &types.TransactWriteItem{
				ConditionCheck: validConditionCheck,
			},
			wantErr: false,
		},
		{
			name: "invalid Put",
			input: &types.TransactWriteItem{
				Put: &types.Put{},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTransactWriteItem(tt.input)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestValidateKeysAndAttributes(t *testing.T) {
	tests := []struct {
		name    string
		input   *types.KeysAndAttributes
		wantErr bool
	}{
		{
			name:    "nil input",
			input:   nil,
			wantErr: false,
		},
		{
			name:    "empty input",
			input:   &types.KeysAndAttributes{},
			wantErr: true,
		},
		{
			name: "valid input",
			input: &types.KeysAndAttributes{
				Keys: []map[string]types.AttributeValue{
					{"id": stringAttr("1")},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateKeysAndAttributes(tt.input)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestValidateGet(t *testing.T) {
	tests := []struct {
		name    string
		input   *types.Get
		wantErr bool
	}{
		{
			name:    "nil input",
			input:   nil,
			wantErr: false,
		},
		{
			name:    "empty input",
			input:   &types.Get{},
			wantErr: true,
		},
		{
			name: "valid input",
			input: &types.Get{
				TableName: aws.String("TestTable"),
				Key:       map[string]types.AttributeValue{"id": stringAttr("1")},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateGet(tt.input)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestValidateWriteRequest(t *testing.T) {
	tests := []struct {
		name    string
		input   *types.WriteRequest
		wantErr bool
	}{
		{
			name:    "nil input",
			input:   nil,
			wantErr: false,
		},
		{
			name:    "empty input",
			input:   &types.WriteRequest{},
			wantErr: false,
		},
		{
			name: "valid PutRequest",
			input: &types.WriteRequest{
				PutRequest: &types.PutRequest{
					Item: map[string]types.AttributeValue{"id": stringAttr("1")},
				},
			},
			wantErr: false,
		},
		{
			name: "valid DeleteRequest",
			input: &types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{
					Key: map[string]types.AttributeValue{"id": stringAttr("1")},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateWriteRequest(tt.input)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

// Operation Input Validation Tests
func TestValidateOpPutItemInput(t *testing.T) {
	tests := []struct {
		name    string
		input   *dynamodb.PutItemInput
		wantErr bool
	}{
		{
			name:    "nil input",
			input:   nil,
			wantErr: false,
		},
		{
			name:    "empty input",
			input:   &dynamodb.PutItemInput{},
			wantErr: true,
		},
		{
			name: "valid input",
			input: &dynamodb.PutItemInput{
				TableName: aws.String("TestTable"),
				Item:      map[string]types.AttributeValue{"id": stringAttr("1")},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateOpPutItemInput(tt.input)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestValidateOpDeleteItemInput(t *testing.T) {
	tests := []struct {
		name    string
		input   *dynamodb.DeleteItemInput
		wantErr bool
	}{
		{
			name:    "nil input",
			input:   nil,
			wantErr: false,
		},
		{
			name:    "empty input",
			input:   &dynamodb.DeleteItemInput{},
			wantErr: true,
		},
		{
			name: "valid input",
			input: &dynamodb.DeleteItemInput{
				TableName: aws.String("TestTable"),
				Key:       map[string]types.AttributeValue{"id": stringAttr("1")},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateOpDeleteItemInput(tt.input)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestValidateOpUpdateItemInput(t *testing.T) {
	tests := []struct {
		name    string
		input   *dynamodb.UpdateItemInput
		wantErr bool
	}{
		{
			name:    "nil input",
			input:   nil,
			wantErr: false,
		},
		{
			name:    "empty input",
			input:   &dynamodb.UpdateItemInput{},
			wantErr: true,
		},
		{
			name: "valid input",
			input: &dynamodb.UpdateItemInput{
				TableName: aws.String("TestTable"),
				Key:       map[string]types.AttributeValue{"id": stringAttr("1")},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateOpUpdateItemInput(tt.input)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestValidateOpGetItemInput(t *testing.T) {
	tests := []struct {
		name    string
		input   *dynamodb.GetItemInput
		wantErr bool
	}{
		{
			name:    "nil input",
			input:   nil,
			wantErr: false,
		},
		{
			name:    "empty input",
			input:   &dynamodb.GetItemInput{},
			wantErr: true,
		},
		{
			name: "valid input",
			input: &dynamodb.GetItemInput{
				TableName: aws.String("TestTable"),
				Key:       map[string]types.AttributeValue{"id": stringAttr("1")},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateOpGetItemInput(tt.input)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestValidateOpQueryInput(t *testing.T) {
	tests := []struct {
		name    string
		input   *dynamodb.QueryInput
		wantErr bool
	}{
		{
			name:    "nil input",
			input:   nil,
			wantErr: false,
		},
		{
			name:    "empty input",
			input:   &dynamodb.QueryInput{},
			wantErr: true,
		},
		{
			name: "valid input with KeyConditions",
			input: &dynamodb.QueryInput{
				TableName: aws.String("TestTable"),
				KeyConditions: map[string]types.Condition{
					"id": {
						ComparisonOperator: "EQ",
						AttributeValueList: []types.AttributeValue{stringAttr("1")},
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateOpQueryInput(tt.input)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestValidateOpScanInput(t *testing.T) {
	tests := []struct {
		name    string
		input   *dynamodb.ScanInput
		wantErr bool
	}{
		{
			name:    "nil input",
			input:   nil,
			wantErr: false,
		},
		{
			name:    "empty input",
			input:   &dynamodb.ScanInput{},
			wantErr: true,
		},
		{
			name: "valid input",
			input: &dynamodb.ScanInput{
				TableName: aws.String("TestTable"),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateOpScanInput(tt.input)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestValidateOpBatchWriteItemInput(t *testing.T) {
	tests := []struct {
		name    string
		input   *dynamodb.BatchWriteItemInput
		wantErr bool
	}{
		{
			name:    "nil input",
			input:   nil,
			wantErr: false,
		},
		{
			name:    "empty input",
			input:   &dynamodb.BatchWriteItemInput{},
			wantErr: true,
		},
		{
			name: "valid input",
			input: &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					"TestTable": {
						{
							PutRequest: &types.PutRequest{
								Item: map[string]types.AttributeValue{"id": stringAttr("1")},
							},
						},
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateOpBatchWriteItemInput(tt.input)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestValidateOpBatchGetItemInput(t *testing.T) {
	tests := []struct {
		name    string
		input   *dynamodb.BatchGetItemInput
		wantErr bool
	}{
		{
			name:    "nil input",
			input:   nil,
			wantErr: false,
		},
		{
			name:    "empty input",
			input:   &dynamodb.BatchGetItemInput{},
			wantErr: true,
		},
		{
			name: "valid input",
			input: &dynamodb.BatchGetItemInput{
				RequestItems: map[string]types.KeysAndAttributes{
					"TestTable": {
						Keys: []map[string]types.AttributeValue{
							{"id": stringAttr("1")},
						},
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateOpBatchGetItemInput(tt.input)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestValidateOpTransactWriteItemsInput(t *testing.T) {
	tests := []struct {
		name    string
		input   *dynamodb.TransactWriteItemsInput
		wantErr bool
	}{
		{
			name:    "nil input",
			input:   nil,
			wantErr: false,
		},
		{
			name:    "empty input",
			input:   &dynamodb.TransactWriteItemsInput{},
			wantErr: true,
		},
		{
			name: "valid input",
			input: &dynamodb.TransactWriteItemsInput{
				TransactItems: []types.TransactWriteItem{
					{
						Put: &types.Put{
							TableName: aws.String("TestTable"),
							Item:      map[string]types.AttributeValue{"id": stringAttr("1")},
						},
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateOpTransactWriteItemsInput(tt.input)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestValidateOpTransactGetItemsInput(t *testing.T) {
	tests := []struct {
		name    string
		input   *dynamodb.TransactGetItemsInput
		wantErr bool
	}{
		{
			name:    "nil input",
			input:   nil,
			wantErr: false,
		},
		{
			name:    "empty input",
			input:   &dynamodb.TransactGetItemsInput{},
			wantErr: true,
		},
		{
			name: "valid input",
			input: &dynamodb.TransactGetItemsInput{
				TransactItems: []types.TransactGetItem{
					{
						Get: &types.Get{
							TableName: aws.String("TestTable"),
							Key:       map[string]types.AttributeValue{"id": stringAttr("1")},
						},
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateOpTransactGetItemsInput(tt.input)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}
