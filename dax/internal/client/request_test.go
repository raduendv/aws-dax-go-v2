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
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestHasDuplicatesWriteRequests(t *testing.T) {
	hk := "hk"
	d := []types.AttributeDefinition{
		{AttributeName: aws.String(hk), AttributeType: types.ScalarAttributeTypeS},
	}
	cases := []struct {
		w []types.WriteRequest
		e bool
	}{
		{
			w: nil,
			e: false,
		},
		{
			w: []types.WriteRequest{},
			e: false,
		},
		{
			w: []types.WriteRequest{
				{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{hk: &types.AttributeValueMemberS{Value: "abc"}}}},
			},
			e: false,
		},
		{
			w: []types.WriteRequest{
				{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{hk: &types.AttributeValueMemberS{Value: "abc"}}}},
				{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{hk: &types.AttributeValueMemberS{Value: "abc"}}}},
			},
			e: true,
		},
		{
			w: []types.WriteRequest{
				{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{hk: &types.AttributeValueMemberS{Value: "abc"}}}},
				{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{hk: &types.AttributeValueMemberS{Value: "def"}}}},
			},
			e: false,
		},
		{
			w: []types.WriteRequest{
				{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{hk: &types.AttributeValueMemberS{Value: "abc"}}}},
				{DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{hk: &types.AttributeValueMemberS{Value: "abc"}}}},
			},
			e: true,
		},
		{
			w: []types.WriteRequest{
				{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{hk: &types.AttributeValueMemberS{Value: "abc"}}}},
				{DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{hk: &types.AttributeValueMemberS{Value: "def"}}}},
			},
			e: false,
		},
		{
			w: []types.WriteRequest{
				{DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{hk: &types.AttributeValueMemberS{Value: "abc"}}}},
				{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{hk: &types.AttributeValueMemberS{Value: "def"}}}},
				{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{hk: &types.AttributeValueMemberS{Value: "xyz"}}}},
				{DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{hk: &types.AttributeValueMemberS{Value: "def"}}}},
			},
			e: true,
		},
	}

	for _, c := range cases {
		a := hasDuplicatesWriteRequests(c.w, d)
		if a != c.e {
			t.Errorf("expected TestHasDuplicatesWriteRequests(%v)=%v, got %v", c.w, c.e, a)
		}
	}
}

func TestHasDuplicateKeysAndAttributes(t *testing.T) {
	hk := "hk"
	d := []types.AttributeDefinition{
		{AttributeName: aws.String(hk), AttributeType: types.ScalarAttributeTypeS},
	}
	cases := []struct {
		kaas types.KeysAndAttributes
		e    bool
	}{
		{
			kaas: types.KeysAndAttributes{},
			e:    false,
		},
		{
			kaas: types.KeysAndAttributes{Keys: []map[string]types.AttributeValue{}},
			e:    false,
		},
		{
			kaas: types.KeysAndAttributes{Keys: []map[string]types.AttributeValue{nil}},
			e:    false,
		},
		{
			kaas: types.KeysAndAttributes{Keys: []map[string]types.AttributeValue{nil, nil, nil}},
			e:    false, // continue with request processing
		},
		{
			kaas: types.KeysAndAttributes{Keys: []map[string]types.AttributeValue{
				{hk: &types.AttributeValueMemberS{Value: "abc"}},
			}},
			e: false,
		},
		{
			kaas: types.KeysAndAttributes{Keys: []map[string]types.AttributeValue{
				{hk: &types.AttributeValueMemberS{Value: "abc"}},
				{hk: &types.AttributeValueMemberS{Value: "def"}},
			}},
			e: false,
		},
		{
			kaas: types.KeysAndAttributes{Keys: []map[string]types.AttributeValue{
				{hk: &types.AttributeValueMemberS{Value: "abc"}},
				{hk: &types.AttributeValueMemberS{Value: "abc"}},
			}},
			e: true,
		},
		{
			kaas: types.KeysAndAttributes{Keys: []map[string]types.AttributeValue{
				{hk: &types.AttributeValueMemberS{Value: "abc"}},
				{hk: &types.AttributeValueMemberS{Value: "def"}},
				{hk: &types.AttributeValueMemberS{Value: "abc"}},
			}},
			e: true,
		},
	}
	for _, c := range cases {
		a := hasDuplicateKeysAndAttributes(c.kaas, d)
		if a != c.e {
			t.Errorf("expected hasDuplicateKeysAndAttributes(%v)=%v, got %v", c.kaas, c.e, a)
		}
	}
}

func reverse(a []interface{}) {
	for i := len(a)/2 - 1; i >= 0; i-- {
		opp := len(a) - 1 - i
		a[i], a[opp] = a[opp], a[i]
	}
}
