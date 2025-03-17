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
	"testing"
	"time"

	"github.com/aws/aws-dax-go-v2/dax/internal/client"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/smithy-go/middleware"
	"github.com/stretchr/testify/assert"
)

func TestConfigMergeFrom(t *testing.T) {
	testCases := []struct {
		testName             string
		daxConfig            Config
		awsConfig            aws.Config
		expectedWriteRetries int
		expectedReadRetries  int
	}{
		{
			testName:             "DefaultConfig merging with an empty aws config should result in keeping the same default retries",
			daxConfig:            DefaultConfig(),
			awsConfig:            aws.Config{},
			expectedWriteRetries: 2,
			expectedReadRetries:  2,
		},
		{
			testName:             "DefaultConfig merging with an aws config that specifies aws.UseServiceDefaultRetries should result in using default retries",
			daxConfig:            DefaultConfig(),
			awsConfig:            aws.Config{RetryMaxAttempts: -1},
			expectedWriteRetries: 2,
			expectedReadRetries:  2,
		},
		{
			testName:             "DefaultConfig merging with an aws config that specifies a non-negative MaxRetry should result in using that value as both WriteRetries and ReadRetries",
			daxConfig:            DefaultConfig(),
			awsConfig:            aws.Config{RetryMaxAttempts: 123},
			expectedWriteRetries: 123,
			expectedReadRetries:  123,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.testName, func(t *testing.T) {
			testCase.daxConfig.mergeFrom(testCase.awsConfig, "")
			if testCase.daxConfig.WriteRetries != testCase.expectedWriteRetries {
				t.Errorf("write retries is %d, but expected %d", testCase.daxConfig.WriteRetries, testCase.expectedWriteRetries)
			}

			if testCase.daxConfig.ReadRetries != testCase.expectedReadRetries {
				t.Errorf("read retries is %d, but expected %d", testCase.daxConfig.ReadRetries, testCase.expectedReadRetries)
			}
		})
	}
}

func TestRequestOptions(t *testing.T) {
	t.Run("read operation with default config", func(t *testing.T) {
		cfg := &Config{
			ReadRetries:  3,
			WriteRetries: 5,
		}

		opts, cfn, err := cfg.requestOptions(true, nil)
		defer func() {
			if cfn != nil {
				cfn()
			}
		}()

		assert.NoError(t, err)
		assert.Equal(t, 3, opts.RetryMaxAttempts)
		assert.Nil(t, cfn)
	})

	t.Run("write operation with default config", func(t *testing.T) {
		cfg := &Config{
			ReadRetries:  3,
			WriteRetries: 5,
		}

		opts, cfn, err := cfg.requestOptions(false, nil)
		defer func() {
			if cfn != nil {
				cfn()
			}
		}()

		assert.NoError(t, err)
		assert.Equal(t, 5, opts.RetryMaxAttempts)
		assert.Nil(t, cfn)
	})

	t.Run("with request timeout", func(t *testing.T) {
		cfg := &Config{
			ReadRetries:    3,
			WriteRetries:   5,
			RequestTimeout: time.Second * 5,
		}

		opts, cfn, err := cfg.requestOptions(true, nil)
		defer func() {
			if cfn != nil {
				cfn()
			}
		}()

		assert.NoError(t, err)
		assert.Equal(t, 3, opts.RetryMaxAttempts)
		assert.NotNil(t, cfn)
	})

	t.Run("with custom context", func(t *testing.T) {
		t.Run("with RequestTimeout", func(t *testing.T) {
			cfg := &Config{
				ReadRetries:    3,
				WriteRetries:   5,
				RequestTimeout: time.Second * 5,
			}

			ctx := context.Background()
			opts, cfn, err := cfg.requestOptions(true, ctx)
			defer func() {
				if cfn != nil {
					cfn()
				}
			}()

			assert.NoError(t, err)
			assert.Equal(t, 3, opts.RetryMaxAttempts)
			assert.NotNil(t, cfn) // Should have CancelFunc due to RequestTimeout
		})

		t.Run("without RequestTimeout", func(t *testing.T) {
			cfg := &Config{
				ReadRetries:  3,
				WriteRetries: 5,
				// No RequestTimeout set
			}

			ctx := context.Background()
			opts, cfn, err := cfg.requestOptions(true, ctx)
			defer func() {
				if cfn != nil {
					cfn()
				}
			}()

			assert.NoError(t, err)
			assert.Equal(t, 3, opts.RetryMaxAttempts)
			assert.Nil(t, cfn) // Should be nil as no timeout is set
		})
	})

	t.Run("with custom middleware should return error", func(t *testing.T) {
		cfg := &Config{
			ReadRetries:  3,
			WriteRetries: 5,
		}

		customOpt := func(o *dynamodb.Options) {
			o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
				return nil
			})
		}

		opts, cfn, err := cfg.requestOptions(true, nil, customOpt)
		defer func() {
			if cfn != nil {
				cfn()
			}
		}()

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "custom middleware through APIOptions is not supported")
		assert.Equal(t, client.RequestOptions{}, opts)
	})
}
