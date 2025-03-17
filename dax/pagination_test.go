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

package dax

import (
	"context"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// MockDaxAPI implements the necessary interfaces for testing
type MockDaxAPI struct {
	queryResults []dynamodb.QueryOutput
	scanResults  []dynamodb.ScanOutput
	queryErr     error
	scanErr      error
	currentQuery int
	currentScan  int
	batchResults []dynamodb.BatchGetItemOutput
	batchErr     error
	currentBatch int
	currentCall  int
	err          error
}

// Query implements the Query method for the interface
func (m *MockDaxAPI) Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	if m.currentQuery >= len(m.queryResults) {
		return nil, nil
	}
	result := m.queryResults[m.currentQuery]
	m.currentQuery++
	return &result, nil
}

// Scan implements the Scan method for the interface
func (m *MockDaxAPI) Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	if m.scanErr != nil {
		return nil, m.scanErr
	}
	if m.currentScan >= len(m.scanResults) {
		return nil, nil
	}
	result := m.scanResults[m.currentScan]
	m.currentScan++
	return &result, nil
}

// BatchGetItem implements the BatchGetItem method for the interface
func (m *MockDaxAPI) BatchGetItem(ctx context.Context, params *dynamodb.BatchGetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	if m.batchErr != nil {
		return nil, m.batchErr
	}
	if m.currentBatch >= len(m.batchResults) {
		return nil, nil
	}
	result := m.batchResults[m.currentBatch]
	m.currentBatch++
	return &result, nil
}

func TestQueryPagination(t *testing.T) {
	// Mock response data
	mockResponses := []dynamodb.QueryOutput{
		{
			Items: []map[string]types.AttributeValue{
				{
					"id":   &types.AttributeValueMemberS{Value: "1"},
					"name": &types.AttributeValueMemberS{Value: "item1"},
				},
			},
			LastEvaluatedKey: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: "1"},
			},
			Count: 1,
		},
		{
			Items: []map[string]types.AttributeValue{
				{
					"id":   &types.AttributeValueMemberS{Value: "2"},
					"name": &types.AttributeValueMemberS{Value: "item2"},
				},
			},
			LastEvaluatedKey: nil, // Last page
			Count:            1,
		},
	}

	// Create mock client
	mockClient := &MockDaxAPI{
		queryResults: mockResponses,
	}

	// Create query input
	input := &dynamodb.QueryInput{
		TableName:              aws.String("TestTable"),
		KeyConditionExpression: aws.String("id = :id"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":id": &types.AttributeValueMemberS{Value: "1"},
		},
		Limit: aws.Int32(1),
	}

	// Create paginator
	paginator := NewQueryPaginator(mockClient, input)

	var allItems []map[string]types.AttributeValue
	pageNum := 0

	// Iterate through pages
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			t.Fatalf("failed to get page %d: %v", pageNum, err)
		}

		allItems = append(allItems, page.Items...)
		pageNum++
	}

	// Verify results
	expectedItems := []map[string]types.AttributeValue{
		{
			"id":   &types.AttributeValueMemberS{Value: "1"},
			"name": &types.AttributeValueMemberS{Value: "item1"},
		},
		{
			"id":   &types.AttributeValueMemberS{Value: "2"},
			"name": &types.AttributeValueMemberS{Value: "item2"},
		},
	}

	if !reflect.DeepEqual(expectedItems, allItems) {
		t.Errorf("Expected items %v, got %v", expectedItems, allItems)
	}

	if pageNum != 2 {
		t.Errorf("Expected 2 pages, got %d", pageNum)
	}
}

func TestScanPagination(t *testing.T) {
	// Mock response data
	mockResponses := []dynamodb.ScanOutput{
		{
			Items: []map[string]types.AttributeValue{
				{
					"id":   &types.AttributeValueMemberS{Value: "1"},
					"name": &types.AttributeValueMemberS{Value: "item1"},
				},
			},
			LastEvaluatedKey: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: "1"},
			},
			Count: 1,
		},
		{
			Items: []map[string]types.AttributeValue{
				{
					"id":   &types.AttributeValueMemberS{Value: "2"},
					"name": &types.AttributeValueMemberS{Value: "item2"},
				},
			},
			LastEvaluatedKey: nil, // Last page
			Count:            1,
		},
	}

	// Create mock client
	mockClient := &MockDaxAPI{
		scanResults: mockResponses,
	}

	// Create scan input
	input := &dynamodb.ScanInput{
		TableName: aws.String("TestTable"),
		Limit:     aws.Int32(1),
	}

	// Create paginator
	paginator := NewScanPaginator(mockClient, input)

	var allItems []map[string]types.AttributeValue
	pageNum := 0

	// Iterate through pages
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			t.Fatalf("failed to get page %d: %v", pageNum, err)
		}

		allItems = append(allItems, page.Items...)
		pageNum++
	}

	// Verify results
	expectedItems := []map[string]types.AttributeValue{
		{
			"id":   &types.AttributeValueMemberS{Value: "1"},
			"name": &types.AttributeValueMemberS{Value: "item1"},
		},
		{
			"id":   &types.AttributeValueMemberS{Value: "2"},
			"name": &types.AttributeValueMemberS{Value: "item2"},
		},
	}

	if !reflect.DeepEqual(expectedItems, allItems) {
		t.Errorf("Expected items %v, got %v", expectedItems, allItems)
	}

	if pageNum != 2 {
		t.Errorf("Expected 2 pages, got %d", pageNum)
	}
}

func TestBatchGetItemPagination(t *testing.T) {
	// Mock response data
	mockResponses := []dynamodb.BatchGetItemOutput{
		{
			Responses: map[string][]map[string]types.AttributeValue{
				"TestTable": {
					{
						"id":   &types.AttributeValueMemberS{Value: "1"},
						"name": &types.AttributeValueMemberS{Value: "item1"},
					},
					{
						"id":   &types.AttributeValueMemberS{Value: "2"},
						"name": &types.AttributeValueMemberS{Value: "item2"},
					},
				},
			},
			UnprocessedKeys: map[string]types.KeysAndAttributes{
				"TestTable": {
					Keys: []map[string]types.AttributeValue{
						{
							"id": &types.AttributeValueMemberS{Value: "3"},
						},
						{
							"id": &types.AttributeValueMemberS{Value: "4"},
						},
					},
				},
			},
		},
		{
			Responses: map[string][]map[string]types.AttributeValue{
				"TestTable": {
					{
						"id":   &types.AttributeValueMemberS{Value: "3"},
						"name": &types.AttributeValueMemberS{Value: "item3"},
					},
					{
						"id":   &types.AttributeValueMemberS{Value: "4"},
						"name": &types.AttributeValueMemberS{Value: "item4"},
					},
				},
			},
			UnprocessedKeys: nil, // Last page
		},
	}

	mockClient := &MockDaxAPI{
		batchResults: mockResponses,
	}

	// Create input
	input := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			"TestTable": {
				Keys: []map[string]types.AttributeValue{
					{
						"id": &types.AttributeValueMemberS{Value: "1"},
					},
					{
						"id": &types.AttributeValueMemberS{Value: "2"},
					},
					{
						"id": &types.AttributeValueMemberS{Value: "3"},
					},
					{
						"id": &types.AttributeValueMemberS{Value: "4"},
					},
				},
			},
		},
	}

	// Create paginator
	paginator := NewBatchGetItemPaginator(mockClient, input)

	var allItems []map[string]types.AttributeValue
	pageNum := 0

	// Iterate through pages
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			t.Fatalf("failed to get page %d: %v", pageNum, err)
		}

		// Collect items from the response
		if items, ok := page.Responses["TestTable"]; ok {
			allItems = append(allItems, items...)
		}
		pageNum++
	}

	// Verify results
	expectedItems := []map[string]types.AttributeValue{
		{
			"id":   &types.AttributeValueMemberS{Value: "1"},
			"name": &types.AttributeValueMemberS{Value: "item1"},
		},
		{
			"id":   &types.AttributeValueMemberS{Value: "2"},
			"name": &types.AttributeValueMemberS{Value: "item2"},
		},
		{
			"id":   &types.AttributeValueMemberS{Value: "3"},
			"name": &types.AttributeValueMemberS{Value: "item3"},
		},
		{
			"id":   &types.AttributeValueMemberS{Value: "4"},
			"name": &types.AttributeValueMemberS{Value: "item4"},
		},
	}

	if !reflect.DeepEqual(expectedItems, allItems) {
		t.Errorf("Expected items %v, got %v", expectedItems, allItems)
	}

	if pageNum != 2 {
		t.Errorf("Expected 2 pages, got %d", pageNum)
	}
}

func TestPaginationWithError(t *testing.T) {
	testCases := []struct {
		name        string
		operation   string
		mockError   error
		expectedErr string
	}{
		{
			name:        "Query with error",
			operation:   "Query",
			mockError:   &types.ResourceNotFoundException{Message: aws.String("Table not found")},
			expectedErr: "ResourceNotFoundException: Table not found",
		},
		{
			name:        "Scan with error",
			operation:   "Scan",
			mockError:   &types.ProvisionedThroughputExceededException{Message: aws.String("Rate exceeded")},
			expectedErr: "ProvisionedThroughputExceededException: Rate exceeded",
		},
		{
			name:        "BatchGetItem with error",
			operation:   "BatchGetItem",
			mockError:   &types.RequestLimitExceeded{Message: aws.String("Request limit exceeded")},
			expectedErr: "RequestLimitExceeded: Request limit exceeded",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var mockClient *MockDaxAPI
			switch tc.operation {
			case "Query":
				mockClient = &MockDaxAPI{queryErr: tc.mockError}
			case "Scan":
				mockClient = &MockDaxAPI{scanErr: tc.mockError}
			case "BatchGetItem":
				mockClient = &MockDaxAPI{batchErr: tc.mockError}
			}

			var err error
			switch tc.operation {
			case "Query":
				input := &dynamodb.QueryInput{
					TableName: aws.String("TestTable"),
				}
				paginator := NewQueryPaginator(mockClient, input)
				_, err = paginator.NextPage(context.TODO())
			case "Scan":
				input := &dynamodb.ScanInput{
					TableName: aws.String("TestTable"),
				}
				paginator := NewScanPaginator(mockClient, input)
				_, err = paginator.NextPage(context.TODO())
			case "BatchGetItem":
				input := &dynamodb.BatchGetItemInput{
					RequestItems: map[string]types.KeysAndAttributes{
						"TestTable": {
							Keys: []map[string]types.AttributeValue{
								{
									"id": &types.AttributeValueMemberS{Value: "1"},
								},
							},
						},
					},
				}
				paginator := NewBatchGetItemPaginator(mockClient, input)
				_, err = paginator.NextPage(context.TODO())
			}

			if err == nil || err.Error() != tc.expectedErr {
				t.Errorf("Expected error %v, got %v", tc.expectedErr, err)
			}
		})
	}
}

// Helper functions for comparing results TODO:move to util.go
func compareAttributeValues(t *testing.T, expected, actual map[string]types.AttributeValue) bool {
	t.Helper()
	if len(expected) != len(actual) {
		t.Errorf("Length mismatch: expected %d, got %d", len(expected), len(actual))
		return false
	}

	for k, v := range expected {
		av, ok := actual[k]
		if !ok {
			t.Errorf("Missing key %s in actual", k)
			return false
		}
		if !reflect.DeepEqual(v, av) {
			t.Errorf("Value mismatch for key %s: expected %v, got %v", k, v, av)
			return false
		}
	}
	return true
}

func compareResponses(t *testing.T, expected, actual map[string][]map[string]types.AttributeValue) bool {
	t.Helper()
	if len(expected) != len(actual) {
		t.Errorf("Length mismatch: expected %d, got %d", len(expected), len(actual))
		return false
	}

	for tableName, expectedItems := range expected {
		actualItems, ok := actual[tableName]
		if !ok {
			t.Errorf("Missing table %s in actual response", tableName)
			return false
		}
		if len(expectedItems) != len(actualItems) {
			t.Errorf("Items length mismatch for table %s: expected %d, got %d",
				tableName, len(expectedItems), len(actualItems))
			return false
		}
		for i := range expectedItems {
			if !compareAttributeValues(t, expectedItems[i], actualItems[i]) {
				return false
			}
		}
	}
	return true
}

func TestBatchGetItemPaginator_NilInput(t *testing.T) {
	mockClient := &MockDaxAPI{}
	paginator := NewBatchGetItemPaginator(mockClient, nil)

	if paginator == nil {
		t.Fatal("Expected non-nil paginator with nil input")
	}
	if !paginator.firstPage {
		t.Error("Expected firstPage to be true")
	}
	if paginator.params == nil {
		t.Error("Expected non-nil params")
	}
}

func TestBatchGetItemPaginator_EmptyResponse(t *testing.T) {
	mockClient := &MockDaxAPI{
		batchResults: []dynamodb.BatchGetItemOutput{
			{
				Responses:       map[string][]map[string]types.AttributeValue{},
				UnprocessedKeys: nil,
			},
		},
	}

	input := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			"TestTable": {
				Keys: []map[string]types.AttributeValue{
					{
						"id": &types.AttributeValueMemberS{Value: "1"},
					},
				},
			},
		},
	}

	paginator := NewBatchGetItemPaginator(mockClient, input)

	output, err := paginator.NextPage(context.TODO())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(output.Responses) != 0 {
		t.Errorf("Expected empty responses, got %v", output.Responses)
	}

	if paginator.HasMorePages() {
		t.Error("Expected no more pages")
	}
}

func TestBatchGetItemPaginator_CustomOptions(t *testing.T) {
	mockClient := &MockDaxAPI{}

	input := &dynamodb.BatchGetItemInput{}

	optFn := func(o *dynamodb.BatchGetItemPaginatorOptions) {
		o.StopOnDuplicateToken = true
	}

	paginator := NewBatchGetItemPaginator(mockClient, input, optFn)

	// Verify the option was set
	if !paginator.options.StopOnDuplicateToken {
		t.Error("Custom option StopOnDuplicateToken not properly set")
	}
}

func TestBatchGetItemPaginator_MultipleOptions(t *testing.T) {
	mockClient := &MockDaxAPI{}

	input := &dynamodb.BatchGetItemInput{}

	optFn := func(o *dynamodb.BatchGetItemPaginatorOptions) {
		o.StopOnDuplicateToken = true
	}

	paginator := NewBatchGetItemPaginator(mockClient, input, optFn)

	// Test the options were set correctly
	if !paginator.options.StopOnDuplicateToken {
		t.Error("StopOnDuplicateToken not set to true")
	}
}

func TestBatchGetItemPaginator_NilOptions(t *testing.T) {
	mockClient := &MockDaxAPI{}
	input := &dynamodb.BatchGetItemInput{}

	paginator := NewBatchGetItemPaginator(mockClient, input)

	// Verify default options
	if paginator.options.StopOnDuplicateToken {
		t.Error("Expected false StopOnDuplicateToken for default options")
	}
}

func TestBatchGetItemPaginator_StopOnDuplicateTokenBehavior(t *testing.T) {
	// Setup mock responses with duplicate data
	mockResponses := []dynamodb.BatchGetItemOutput{
		{
			Responses: map[string][]map[string]types.AttributeValue{
				"TestTable": {
					{
						"id": &types.AttributeValueMemberS{Value: "1"},
					},
				},
			},
			UnprocessedKeys: map[string]types.KeysAndAttributes{
				"TestTable": {
					Keys: []map[string]types.AttributeValue{
						{
							"id": &types.AttributeValueMemberS{Value: "1"}, // Duplicate key
						},
					},
				},
			},
		},
	}

	mockClient := &MockDaxAPI{
		batchResults: mockResponses,
	}

	input := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			"TestTable": {
				Keys: []map[string]types.AttributeValue{
					{
						"id": &types.AttributeValueMemberS{Value: "1"},
					},
				},
			},
		},
	}

	// Test with StopOnDuplicateToken set to true
	optFn := func(o *dynamodb.BatchGetItemPaginatorOptions) {
		o.StopOnDuplicateToken = true
	}

	paginator := NewBatchGetItemPaginator(mockClient, input, optFn)

	// First call should succeed
	_, err := paginator.NextPage(context.TODO())
	if err != nil {
		t.Fatalf("Unexpected error on first page: %v", err)
	}

	// Second call should not proceed due to StopOnDuplicateToken
	if paginator.HasMorePages() {
		t.Error("Expected no more pages due to StopOnDuplicateToken")
	}
}

func TestBatchGetItemPaginator_MultiplePages(t *testing.T) {
	mockResponses := []dynamodb.BatchGetItemOutput{
		{
			Responses: map[string][]map[string]types.AttributeValue{
				"TestTable": {
					{
						"id":   &types.AttributeValueMemberS{Value: "1"},
						"name": &types.AttributeValueMemberS{Value: "item1"},
					},
					{
						"id":   &types.AttributeValueMemberS{Value: "2"},
						"name": &types.AttributeValueMemberS{Value: "item2"},
					},
				},
			},
			UnprocessedKeys: map[string]types.KeysAndAttributes{
				"TestTable": {
					Keys: []map[string]types.AttributeValue{
						{
							"id": &types.AttributeValueMemberS{Value: "3"},
						},
						{
							"id": &types.AttributeValueMemberS{Value: "4"},
						},
					},
				},
			},
		},
		{
			Responses: map[string][]map[string]types.AttributeValue{
				"TestTable": {
					{
						"id":   &types.AttributeValueMemberS{Value: "3"},
						"name": &types.AttributeValueMemberS{Value: "item3"},
					},
					{
						"id":   &types.AttributeValueMemberS{Value: "4"},
						"name": &types.AttributeValueMemberS{Value: "item4"},
					},
				},
			},
			UnprocessedKeys: nil, // Last page
		},
	}

	mockClient := &MockDaxAPI{
		batchResults: mockResponses,
	}

	input := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			"TestTable": {
				Keys: []map[string]types.AttributeValue{
					{
						"id": &types.AttributeValueMemberS{Value: "1"},
					},
					{
						"id": &types.AttributeValueMemberS{Value: "2"},
					},
					{
						"id": &types.AttributeValueMemberS{Value: "3"},
					},
					{
						"id": &types.AttributeValueMemberS{Value: "4"},
					},
				},
			},
		},
	}

	paginator := NewBatchGetItemPaginator(mockClient, input)

	var allItems []map[string]types.AttributeValue
	pageNum := 0

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			t.Fatalf("failed to get page %d: %v", pageNum, err)
		}

		if items, ok := page.Responses["TestTable"]; ok {
			allItems = append(allItems, items...)
		}
		pageNum++
	}

	expectedItems := []map[string]types.AttributeValue{
		{
			"id":   &types.AttributeValueMemberS{Value: "1"},
			"name": &types.AttributeValueMemberS{Value: "item1"},
		},
		{
			"id":   &types.AttributeValueMemberS{Value: "2"},
			"name": &types.AttributeValueMemberS{Value: "item2"},
		},
		{
			"id":   &types.AttributeValueMemberS{Value: "3"},
			"name": &types.AttributeValueMemberS{Value: "item3"},
		},
		{
			"id":   &types.AttributeValueMemberS{Value: "4"},
			"name": &types.AttributeValueMemberS{Value: "item4"},
		},
	}

	if !reflect.DeepEqual(expectedItems, allItems) {
		t.Errorf("Expected items %v, got %v", expectedItems, allItems)
	}

	if pageNum != 2 {
		t.Errorf("Expected 2 pages, got %d", pageNum)
	}
}

func TestBatchGetItemPaginator_UnprocessedKeysHandling(t *testing.T) {
	mockResponses := []dynamodb.BatchGetItemOutput{
		{
			Responses: map[string][]map[string]types.AttributeValue{
				"TestTable": {
					{
						"id": &types.AttributeValueMemberS{Value: "1"},
					},
				},
			},
			UnprocessedKeys: map[string]types.KeysAndAttributes{
				"TestTable": {
					Keys: []map[string]types.AttributeValue{
						{
							"id": &types.AttributeValueMemberS{Value: "2"},
						},
					},
				},
			},
		},
		{
			Responses: map[string][]map[string]types.AttributeValue{
				"TestTable": {
					{
						"id": &types.AttributeValueMemberS{Value: "2"},
					},
				},
			},
		},
	}

	mockClient := &MockDaxAPI{
		batchResults: mockResponses,
	}

	input := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			"TestTable": {
				Keys: []map[string]types.AttributeValue{
					{
						"id": &types.AttributeValueMemberS{Value: "1"},
					},
					{
						"id": &types.AttributeValueMemberS{Value: "2"},
					},
				},
			},
		},
	}

	paginator := NewBatchGetItemPaginator(mockClient, input)

	var processedItems int
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(context.TODO())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		processedItems += len(output.Responses["TestTable"])
	}

	if processedItems != 2 {
		t.Errorf("Expected 2 processed items, got %d", processedItems)
	}
}

// Query Paginator Tests
func TestQueryPaginator_NilInput(t *testing.T) {
	mockClient := &MockDaxAPI{}
	paginator := NewQueryPaginator(mockClient, nil)

	if paginator == nil {
		t.Fatal("Expected non-nil paginator with nil input")
	}
	if !paginator.firstPage {
		t.Error("Expected firstPage to be true")
	}
	if paginator.params == nil {
		t.Error("Expected non-nil params")
	}
}

func TestQueryPaginator_EmptyResponse(t *testing.T) {
	mockClient := &MockDaxAPI{
		queryResults: []dynamodb.QueryOutput{
			{
				Items: []map[string]types.AttributeValue{},
			},
		},
	}

	input := &dynamodb.QueryInput{
		TableName:              aws.String("TestTable"),
		KeyConditionExpression: aws.String("id = :id"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":id": &types.AttributeValueMemberS{Value: "1"},
		},
	}

	paginator := NewQueryPaginator(mockClient, input)

	output, err := paginator.NextPage(context.TODO())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(output.Items) != 0 {
		t.Errorf("Expected empty items, got %v", output.Items)
	}

	if paginator.HasMorePages() {
		t.Error("Expected no more pages")
	}
}

func TestQueryPaginator_MultiplePages(t *testing.T) {
	mockResponses := []dynamodb.QueryOutput{
		{
			Items: []map[string]types.AttributeValue{
				{
					"id":   &types.AttributeValueMemberS{Value: "1"},
					"name": &types.AttributeValueMemberS{Value: "item1"},
				},
				{
					"id":   &types.AttributeValueMemberS{Value: "2"},
					"name": &types.AttributeValueMemberS{Value: "item2"},
				},
			},
			LastEvaluatedKey: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: "2"},
			},
		},
		{
			Items: []map[string]types.AttributeValue{
				{
					"id":   &types.AttributeValueMemberS{Value: "3"},
					"name": &types.AttributeValueMemberS{Value: "item3"},
				},
			},
			LastEvaluatedKey: nil, // Last page
		},
	}

	mockClient := &MockDaxAPI{
		queryResults: mockResponses,
	}

	input := &dynamodb.QueryInput{
		TableName:              aws.String("TestTable"),
		KeyConditionExpression: aws.String("id BETWEEN :start AND :end"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":start": &types.AttributeValueMemberS{Value: "1"},
			":end":   &types.AttributeValueMemberS{Value: "3"},
		},
	}

	paginator := NewQueryPaginator(mockClient, input)

	var allItems []map[string]types.AttributeValue
	pageNum := 0

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			t.Fatalf("failed to get page %d: %v", pageNum, err)
		}

		allItems = append(allItems, page.Items...)
		pageNum++
	}

	expectedItems := []map[string]types.AttributeValue{
		{
			"id":   &types.AttributeValueMemberS{Value: "1"},
			"name": &types.AttributeValueMemberS{Value: "item1"},
		},
		{
			"id":   &types.AttributeValueMemberS{Value: "2"},
			"name": &types.AttributeValueMemberS{Value: "item2"},
		},
		{
			"id":   &types.AttributeValueMemberS{Value: "3"},
			"name": &types.AttributeValueMemberS{Value: "item3"},
		},
	}

	if !reflect.DeepEqual(expectedItems, allItems) {
		t.Errorf("Expected items %v, got %v", expectedItems, allItems)
	}

	if pageNum != 2 {
		t.Errorf("Expected 2 pages, got %d", pageNum)
	}
}

// Scan Paginator Tests
func TestScanPaginator_NilInput(t *testing.T) {
	mockClient := &MockDaxAPI{}
	paginator := NewScanPaginator(mockClient, nil)

	if paginator == nil {
		t.Fatal("Expected non-nil paginator with nil input")
	}
	if !paginator.firstPage {
		t.Error("Expected firstPage to be true")
	}
	if paginator.params == nil {
		t.Error("Expected non-nil params")
	}
}

func TestScanPaginator_EmptyResponse(t *testing.T) {
	mockClient := &MockDaxAPI{
		scanResults: []dynamodb.ScanOutput{
			{
				Items: []map[string]types.AttributeValue{},
			},
		},
	}

	input := &dynamodb.ScanInput{
		TableName: aws.String("TestTable"),
	}

	paginator := NewScanPaginator(mockClient, input)

	output, err := paginator.NextPage(context.TODO())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(output.Items) != 0 {
		t.Errorf("Expected empty items, got %v", output.Items)
	}

	if paginator.HasMorePages() {
		t.Error("Expected no more pages")
	}
}

func TestScanPaginator_MultiplePages(t *testing.T) {
	mockResponses := []dynamodb.ScanOutput{
		{
			Items: []map[string]types.AttributeValue{
				{
					"id":   &types.AttributeValueMemberS{Value: "1"},
					"name": &types.AttributeValueMemberS{Value: "item1"},
				},
				{
					"id":   &types.AttributeValueMemberS{Value: "2"},
					"name": &types.AttributeValueMemberS{Value: "item2"},
				},
			},
			LastEvaluatedKey: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: "2"},
			},
		},
		{
			Items: []map[string]types.AttributeValue{
				{
					"id":   &types.AttributeValueMemberS{Value: "3"},
					"name": &types.AttributeValueMemberS{Value: "item3"},
				},
			},
			LastEvaluatedKey: nil, // Last page
		},
	}

	mockClient := &MockDaxAPI{
		scanResults: mockResponses,
	}

	input := &dynamodb.ScanInput{
		TableName: aws.String("TestTable"),
	}

	paginator := NewScanPaginator(mockClient, input)

	var allItems []map[string]types.AttributeValue
	pageNum := 0

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			t.Fatalf("failed to get page %d: %v", pageNum, err)
		}

		allItems = append(allItems, page.Items...)
		pageNum++
	}

	expectedItems := []map[string]types.AttributeValue{
		{
			"id":   &types.AttributeValueMemberS{Value: "1"},
			"name": &types.AttributeValueMemberS{Value: "item1"},
		},
		{
			"id":   &types.AttributeValueMemberS{Value: "2"},
			"name": &types.AttributeValueMemberS{Value: "item2"},
		},
		{
			"id":   &types.AttributeValueMemberS{Value: "3"},
			"name": &types.AttributeValueMemberS{Value: "item3"},
		},
	}

	if !reflect.DeepEqual(expectedItems, allItems) {
		t.Errorf("Expected items %v, got %v", expectedItems, allItems)
	}

	if pageNum != 2 {
		t.Errorf("Expected 2 pages, got %d", pageNum)
	}
}
