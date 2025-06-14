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
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-dax-go-v2/dax/internal/cbor"
	"github.com/aws/aws-dax-go-v2/dax/internal/lru"
	"github.com/aws/aws-dax-go-v2/dax/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"
	"github.com/aws/smithy-go/metrics"
)

const (
	userAgent  = "DaxGoClient-1.0.0"
	daxAddress = "https://dax.amazonaws.com"

	authTtlSecs          = 5 * 60
	tubeAuthWindowScalar = 0.75

	emptyAttributeListId = 1
)

const (
	serviceName             = "dax"
	opDefineAttributeList   = "DefineAttributeList"
	opDefineAttributeListId = "DefineAttributeListId"
	opDefineKeySchema       = "DefineKeySchema"
	opEndpoints             = "Endpoints"
	OpGetItem               = "GetItem"
	OpPutItem               = "PutItem"
	OpUpdateItem            = "UpdateItem"
	OpDeleteItem            = "DeleteItem"
	OpBatchGetItem          = "BatchGetItem"
	OpBatchWriteItem        = "BatchWriteItem"
	OpTransactGetItems      = "TransactGetItems"
	OpTransactWriteItems    = "TransactWriteItems"
	OpQuery                 = "Query"
	OpScan                  = "Scan"
)

const (
	keySchemaLruCacheSize     = 100
	attributeListLruCacheSize = 1000
)

type SingleDaxClient struct {
	region             string
	credentials        aws.CredentialsProvider
	tubeAuthWindowSecs int64
	executor           *taskExecutor

	pool              *tubePool
	keySchema         *lru.Lru
	attrNamesListToId *lru.Lru
	attrListIdToNames *lru.Lru

	healthStatus HealthStatus

	daxSdkMetrics *daxSdkMetrics
}

func NewSingleClient(endpoint string, connConfigData connConfig, region string, credentials aws.CredentialsProvider, routeListener RouteListener, sdkMetrics *daxSdkMetrics) (*SingleDaxClient, error) {
	return newSingleClientWithOptions(endpoint, connConfigData, region, credentials, -1, defaultDialer.DialContext, routeListener, sdkMetrics)
}

func newSingleClientWithOptions(
	endpoint string,
	connConfigData connConfig,
	region string,
	credentials aws.CredentialsProvider,
	maxPendingConnections int,
	dialContextFn dialContext,
	routeListener RouteListener,
	sdkMetrics *daxSdkMetrics,
) (*SingleDaxClient, error) {
	po := defaultTubePoolOptions
	if maxPendingConnections > 0 {
		po.maxConcurrentConnAttempts = maxPendingConnections
	}

	if sdkMetrics == nil {
		var err error
		sdkMetrics, err = buildDaxSdkMetrics(&metrics.NopMeterProvider{})
		if err != nil {
			return nil, err
		}
	}

	po.dialContext = dialContextFn

	client := &SingleDaxClient{
		region:             region,
		credentials:        credentials,
		tubeAuthWindowSecs: authTtlSecs * tubeAuthWindowScalar,
		pool:               newTubePoolWithOptions(endpoint, po, connConfigData, sdkMetrics),
		executor:           newExecutor(),
		healthStatus:       newHealthStatus(endpoint, routeListener),
		daxSdkMetrics:      sdkMetrics,
	}

	client.keySchema = &lru.Lru{
		MaxEntries: keySchemaLruCacheSize,
		LoadFunc: func(ctx context.Context, key lru.Key) (interface{}, error) {
			table, ok := key.(string)
			if !ok {
				return nil, &smithy.SerializationError{Err: errors.New("unexpected type for table name")}
			}
			if ctx == nil {
				ctx = context.Background()
			}
			return client.defineKeySchema(ctx, table)
		},
	}

	client.attrNamesListToId = &lru.Lru{
		MaxEntries: attributeListLruCacheSize,
		LoadFunc: func(ctx context.Context, key lru.Key) (interface{}, error) {
			attrNames, ok := key.([]string)
			if !ok {
				return nil, &smithy.SerializationError{Err: errors.New("unexpected type for attribute list")}
			}
			if ctx == nil {
				ctx = context.Background()
			}
			return client.defineAttributeListId(ctx, attrNames)
		},
		KeyMarshaller: func(key lru.Key) lru.Key {
			var buf bytes.Buffer
			w := cbor.NewWriter(&buf)
			defer w.Close()
			for _, v := range key.([]string) {
				w.WriteString(v)
			}
			w.Flush()
			return string(buf.Bytes())
		},
	}

	client.attrListIdToNames = &lru.Lru{
		MaxEntries: attributeListLruCacheSize,
		LoadFunc: func(ctx context.Context, key lru.Key) (interface{}, error) {
			id, ok := key.(int64)
			if !ok {
				return nil, &smithy.SerializationError{Err: errors.New("unexpected type for attribute list id")}
			}
			if ctx == nil {
				ctx = context.Background()
			}
			return client.defineAttributeList(ctx, id)
		},
	}

	return client, nil
}

func (client *SingleDaxClient) Close() error {
	client.executor.stopAll()
	if client.pool != nil {
		return client.pool.Close()
	}
	return nil
}

func (client *SingleDaxClient) startHealthChecks(cc *cluster, host hostPort) {
	cc.debugLog("Starting health checks for :: " + host.host)
	client.executor.start(cc.config.ClientHealthCheckInterval, func() error {
		ctx, cfn := context.WithTimeout(context.Background(), 1*time.Second)
		defer cfn()
		var err error
		opts := RequestOptions{}
		opts.RetryMaxAttempts = 3
		_, err = client.endpoints(ctx, opts)
		if err != nil {
			cc.debugLog("Health checks failed with error " + err.Error() + " for host :: " + host.host)
			cc.onHealthCheckFailed(host)
		} else {
			client.healthStatus.onHealthCheckSuccess(client)
			cc.debugLog("Health checks succeeded for host:: " + host.host)
		}
		return nil
	})
}

func (client *SingleDaxClient) endpoints(ctx context.Context, opt RequestOptions) ([]serviceEndpoint, error) {
	encoder := func(writer *cbor.Writer) error {
		return encodeEndpointsInput(writer)
	}
	var out []serviceEndpoint
	var err error
	decoder := func(reader *cbor.Reader) error {
		out, err = decodeEndpointsOutput(reader)
		return err
	}
	if err = client.executeWithRetries(ctx, opEndpoints, opt, encoder, decoder); err != nil {
		return nil, err
	}
	return out, nil
}

func (client *SingleDaxClient) defineAttributeListId(ctx context.Context, attrNames []string) (int64, error) {
	if len(attrNames) == 0 {
		return emptyAttributeListId, nil
	}
	encoder := func(writer *cbor.Writer) error {
		return encodeDefineAttributeListIdInput(attrNames, writer)
	}
	var out int64
	var err error
	decoder := func(reader *cbor.Reader) error {
		out, err = decodeDefineAttributeListIdOutput(reader)
		return err
	}
	opt := RequestOptions{}
	if err = client.executeWithRetries(ctx, opDefineAttributeListId, opt, encoder, decoder); err != nil {
		return 0, err
	}
	return out, nil
}

func (client *SingleDaxClient) defineAttributeList(ctx context.Context, id int64) ([]string, error) {
	if id == emptyAttributeListId {
		return []string{}, nil
	}
	encoder := func(writer *cbor.Writer) error {
		return encodeDefineAttributeListInput(id, writer)
	}
	var out []string
	var err error
	decoder := func(reader *cbor.Reader) error {
		out, err = decodeDefineAttributeListOutput(reader)
		return err
	}
	opt := RequestOptions{}
	if err = client.executeWithRetries(ctx, opDefineAttributeList, opt, encoder, decoder); err != nil {
		return nil, err
	}
	return out, nil
}

func (client *SingleDaxClient) defineKeySchema(ctx context.Context, table string) ([]types.AttributeDefinition, error) {
	encoder := func(writer *cbor.Writer) error {
		return encodeDefineKeySchemaInput(table, writer)
	}
	var out []types.AttributeDefinition
	var err error
	decoder := func(reader *cbor.Reader) error {
		out, err = decodeDefineKeySchemaOutput(reader)
		return err
	}
	opt := RequestOptions{}
	if err = client.executeWithRetries(ctx, opDefineKeySchema, opt, encoder, decoder); err != nil {
		return nil, err
	}
	return out, nil
}

func (client *SingleDaxClient) PutItemWithOptions(ctx context.Context, input *dynamodb.PutItemInput, output *dynamodb.PutItemOutput, opt RequestOptions) (*dynamodb.PutItemOutput, error) {
	encoder := func(writer *cbor.Writer) error {
		return encodePutItemInput(ctx, input, client.keySchema, client.attrNamesListToId, writer)
	}
	var err error
	decoder := func(reader *cbor.Reader) error {
		output, err = decodePutItemOutput(ctx, reader, input, client.keySchema, client.attrListIdToNames, output)
		return err
	}

	if err = client.executeWithRetries(ctx, OpPutItem, opt, encoder, decoder); err != nil {
		return output, err
	}
	return output, nil
}

func (client *SingleDaxClient) DeleteItemWithOptions(ctx context.Context, input *dynamodb.DeleteItemInput, output *dynamodb.DeleteItemOutput, opt RequestOptions) (*dynamodb.DeleteItemOutput, error) {
	encoder := func(writer *cbor.Writer) error {
		return encodeDeleteItemInput(ctx, input, client.keySchema, writer)
	}
	var err error
	decoder := func(reader *cbor.Reader) error {
		output, err = decodeDeleteItemOutput(ctx, reader, input, client.keySchema, client.attrListIdToNames, output)
		return err
	}
	if err = client.executeWithRetries(ctx, OpDeleteItem, opt, encoder, decoder); err != nil {
		return output, err
	}
	return output, nil
}

func (client *SingleDaxClient) UpdateItemWithOptions(ctx context.Context, input *dynamodb.UpdateItemInput, output *dynamodb.UpdateItemOutput, opt RequestOptions) (*dynamodb.UpdateItemOutput, error) {
	encoder := func(writer *cbor.Writer) error {
		return encodeUpdateItemInput(ctx, input, client.keySchema, writer)
	}
	var err error
	decoder := func(reader *cbor.Reader) error {
		output, err = decodeUpdateItemOutput(ctx, reader, input, client.keySchema, client.attrListIdToNames, output)
		return err
	}
	if err = client.executeWithRetries(ctx, OpUpdateItem, opt, encoder, decoder); err != nil {
		return output, err
	}
	return output, nil
}

func (client *SingleDaxClient) GetItemWithOptions(ctx context.Context, input *dynamodb.GetItemInput, output *dynamodb.GetItemOutput, opt RequestOptions) (*dynamodb.GetItemOutput, error) {
	encoder := func(writer *cbor.Writer) error {
		return encodeGetItemInput(ctx, input, client.keySchema, writer)
	}
	var err error
	decoder := func(reader *cbor.Reader) error {
		output, err = decodeGetItemOutput(ctx, reader, input, client.attrListIdToNames, output)
		return err
	}
	if err = client.executeWithRetries(ctx, OpGetItem, opt, encoder, decoder); err != nil {
		client.healthStatus.onErrorInReadRequest(err, client)
		return output, err
	}
	client.healthStatus.onSuccessInReadRequest()
	return output, nil
}

func (client *SingleDaxClient) ScanWithOptions(ctx context.Context, input *dynamodb.ScanInput, output *dynamodb.ScanOutput, opt RequestOptions) (*dynamodb.ScanOutput, error) {
	encoder := func(writer *cbor.Writer) error {
		return encodeScanInput(ctx, input, client.keySchema, writer)
	}
	var err error
	decoder := func(reader *cbor.Reader) error {
		output, err = decodeScanOutput(ctx, reader, input, client.keySchema, client.attrListIdToNames, output)
		return err
	}
	if err = client.executeWithRetries(ctx, OpScan, opt, encoder, decoder); err != nil {
		client.healthStatus.onErrorInReadRequest(err, client)
		return output, err
	}
	client.healthStatus.onSuccessInReadRequest()
	return output, nil
}

func (client *SingleDaxClient) QueryWithOptions(ctx context.Context, input *dynamodb.QueryInput, output *dynamodb.QueryOutput, opt RequestOptions) (*dynamodb.QueryOutput, error) {
	encoder := func(writer *cbor.Writer) error {
		return encodeQueryInput(ctx, input, client.keySchema, writer)
	}
	var err error
	decoder := func(reader *cbor.Reader) error {
		output, err = decodeQueryOutput(ctx, reader, input, client.keySchema, client.attrListIdToNames, output)
		return err
	}
	if err = client.executeWithRetries(ctx, OpQuery, opt, encoder, decoder); err != nil {
		client.healthStatus.onErrorInReadRequest(err, client)
		return output, err
	}
	client.healthStatus.onSuccessInReadRequest()
	return output, nil
}

func (client *SingleDaxClient) BatchWriteItemWithOptions(ctx context.Context, input *dynamodb.BatchWriteItemInput, output *dynamodb.BatchWriteItemOutput, opt RequestOptions) (*dynamodb.BatchWriteItemOutput, error) {
	encoder := func(writer *cbor.Writer) error {
		return encodeBatchWriteItemInput(ctx, input, client.keySchema, client.attrNamesListToId, writer)
	}
	var err error
	decoder := func(reader *cbor.Reader) error {
		output, err = decodeBatchWriteItemOutput(ctx, reader, client.keySchema, client.attrListIdToNames, output)
		return err
	}
	if err = client.executeWithRetries(ctx, OpBatchWriteItem, opt, encoder, decoder); err != nil {
		return output, err
	}
	return output, nil
}

func (client *SingleDaxClient) BatchGetItemWithOptions(ctx context.Context, input *dynamodb.BatchGetItemInput, output *dynamodb.BatchGetItemOutput, opt RequestOptions) (*dynamodb.BatchGetItemOutput, error) {
	encoder := func(writer *cbor.Writer) error {
		return encodeBatchGetItemInput(ctx, input, client.keySchema, writer)
	}
	var err error
	decoder := func(reader *cbor.Reader) error {
		output, err = decodeBatchGetItemOutput(ctx, reader, input, client.keySchema, client.attrListIdToNames, output)
		return err
	}
	if err = client.executeWithRetries(ctx, OpBatchGetItem, opt, encoder, decoder); err != nil {
		client.healthStatus.onErrorInReadRequest(err, client)
		return output, err
	}
	client.healthStatus.onSuccessInReadRequest()
	return output, nil
}

func (client *SingleDaxClient) TransactWriteItemsWithOptions(ctx context.Context, input *dynamodb.TransactWriteItemsInput, output *dynamodb.TransactWriteItemsOutput, opt RequestOptions) (*dynamodb.TransactWriteItemsOutput, error) {
	extractedKeys := make([]map[string]types.AttributeValue, len(input.TransactItems))
	encoder := func(writer *cbor.Writer) error {
		return encodeTransactWriteItemsInput(ctx, input, client.keySchema, client.attrNamesListToId, writer, extractedKeys)
	}
	var err error
	decoder := func(reader *cbor.Reader) error {
		output, err = decodeTransactWriteItemsOutput(ctx, reader, input, client.keySchema, client.attrListIdToNames, output)
		return err
	}
	if err = client.executeWithRetries(ctx, OpTransactWriteItems, opt, encoder, decoder); err != nil {
		if failure, ok := err.(*daxTransactionCanceledFailure); ok {
			var cancellationReasons []types.CancellationReason
			if cancellationReasons, err = decodeTransactionCancellationReasons(ctx, failure, extractedKeys, client.attrListIdToNames); err != nil {
				return output, err
			}
			failure.cancellationReasons = cancellationReasons
			return output, failure
		}
		return output, err
	}
	return output, nil
}

func (client *SingleDaxClient) TransactGetItemsWithOptions(ctx context.Context, input *dynamodb.TransactGetItemsInput, output *dynamodb.TransactGetItemsOutput, opt RequestOptions) (*dynamodb.TransactGetItemsOutput, error) {
	extractedKeys := make([]map[string]types.AttributeValue, len(input.TransactItems))
	encoder := func(writer *cbor.Writer) error {
		return encodeTransactGetItemsInput(ctx, input, client.keySchema, writer, extractedKeys)
	}
	var err error
	decoder := func(reader *cbor.Reader) error {
		output, err = decodeTransactGetItemsOutput(ctx, reader, input, client.keySchema, client.attrListIdToNames, output)
		return err
	}
	if err = client.executeWithRetries(ctx, OpTransactGetItems, opt, encoder, decoder); err != nil {
		if failure, ok := err.(*daxTransactionCanceledFailure); ok {
			var cancellationReasons []types.CancellationReason
			if cancellationReasons, err = decodeTransactionCancellationReasons(ctx, failure, extractedKeys, client.attrListIdToNames); err != nil {
				return output, err
			}
			failure.cancellationReasons = cancellationReasons
			return output, failure
		}
		return output, err
	}
	return output, nil
}

func (client *SingleDaxClient) newContext(ctx context.Context, o RequestOptions) context.Context {
	if o.Context != nil {
		return o.Context
	}
	if ctx != nil {
		return ctx
	}
	return context.Background()
}

func (client *SingleDaxClient) executeWithRetries(ctx context.Context, op string, o RequestOptions, encoder func(writer *cbor.Writer) error, decoder func(reader *cbor.Reader) error) error {
	ctx = client.newContext(ctx, o)

	var err error
	attempts := o.RetryMaxAttempts
	// Start from 0 to accommodate for the initial request
	for i := 0; i <= attempts; i++ {
		if i > 0 && o.Logger != nil && o.LogLevel.Matches(utils.LogDebugWithRequestRetries) {
			o.Logger.Logf(logging.Debug, "Retrying Request %s/%s, attempt %d", service, op, i)
		}

		err = client.executeWithContext(ctx, op, encoder, decoder, o)
		if err == nil {
			return nil
		}

		if errors.Is(err, context.Canceled) {
			return &smithy.CanceledError{Err: err}
		}

		if i != attempts {
			delay := o.RetryDelay
			if sleepErr := SleepWithContext(ctx, op, delay); sleepErr != nil {
				return &smithy.OperationError{Err: sleepErr, ServiceID: service, OperationName: op}
			}

			if o.Logger != nil && o.LogLevel.Matches(utils.LogDebugWithRequestRetries) {
				o.Logger.Logf(logging.Debug, "Error in executing %s%s : %s", service, op, err)
			}
		}
	}
	// Return the last error occurred
	return translateError(err)
}

func (client *SingleDaxClient) executeWithContext(ctx context.Context, op string, encoder func(writer *cbor.Writer) error, decoder func(reader *cbor.Reader) error, opt RequestOptions) (out error) {
	startTime := time.Now()

	defer func() {
		histogramMicrosecondsInt64(ctx, client.daxSdkMetrics, fmt.Sprintf(daxOpNameLatencyUs, op), startTime)

		if out != nil {
			countMetricInt64(ctx, client.daxSdkMetrics, fmt.Sprintf(daxOpNameFailure, op), 1)

			return
		}

		countMetricInt64(ctx, client.daxSdkMetrics, fmt.Sprintf(daxOpNameSuccess, op), 1)
	}()

	t, err := client.pool.getWithContext(ctx, client.isHighPriority(op), opt)
	if err != nil {
		return err
	}
	if err = client.pool.setDeadline(ctx, t); err != nil {
		// If the error is just due to context cancelled or timeout
		// then the tube is still usable because we have not written anything to tube
		if err == ctx.Err() {
			client.pool.put(t)
			return err
		}
		// If we get error while setting deadline of tube
		// probably something is wrong with the tube
		client.pool.closeTube(t)
		return err
	}

	if err = client.auth(ctx, t); err != nil {
		// Auth method writes in the tube and
		// it is not guaranteed that it will be drained completely on error
		client.pool.closeTube(t)
		return err
	}

	writer := t.CborWriter()
	if err = encoder(writer); err != nil {
		// Validation errors will cause connection to be closed as there is no guarantee
		// that the validation was performed before any data was written into tube
		client.pool.closeTube(t)
		return err
	}

	// actual request is sent here
	if err := writer.Flush(); err != nil {
		client.pool.closeTube(t)

		return err
	}

	reader := t.CborReader()
	ex, err := decodeError(reader)

	if err != nil { // decode or network error - doesn't guarantee completely drained tube
		client.pool.closeTube(t)
		return err
	}
	if ex != nil { // user or server error
		client.recycleTube(t, ex)
		return ex
	}

	err = decoder(reader)
	if err != nil {
		// we are not able to completely drain tube
		client.pool.closeTube(t)
	} else {
		client.pool.put(t)
	}

	return err
}

func (client *SingleDaxClient) isHighPriority(op string) bool {
	switch op {
	case opDefineAttributeListId, opDefineAttributeList, opDefineKeySchema:
		return true
	default:
		return false
	}
}

func (client *SingleDaxClient) recycleTube(t tube, err error) {
	if t == nil {
		return
	}

	var recycle bool
	if err == nil {
		recycle = true
	} else {
		// IO streams are guaranteed to be completely drained only on daxRequestException
		d, ok := err.(*daxRequestFailure)
		recycle = ok
		if ok && d.authError() {
			t.SetAuthExpiryUnix(time.Now().Unix())
		}
	}
	if recycle {
		client.pool.put(t)
	} else {
		client.pool.closeTube(t)
	}
}
func (client *SingleDaxClient) auth(ctx context.Context, t tube) error {
	// TODO credentials.Get() cause a throughput drop of ~25 with 250 goroutines with DefaultCredentialChain (only instance profile credentials available)

	creds, err := client.credentials.Retrieve(ctx)

	if err != nil {
		return err
	}

	now := time.Now().UTC()
	if t.CompareAndSwapAuthID(creds.AccessKeyID) || t.AuthExpiryUnix() <= now.Unix() {
		stringToSign, signature := generateSigV4WithTime(creds, daxAddress, client.region, "", now)
		writer := t.CborWriter()

		if err := encodeAuthInput(creds.AccessKeyID, creds.SessionToken, stringToSign, signature, userAgent, writer); err != nil {
			return err
		}

		if err := writer.Flush(); err != nil {
			return err
		}

		t.SetAuthExpiryUnix(now.Unix() + client.tubeAuthWindowSecs)
	}

	return nil
}

func (client *SingleDaxClient) reapIdleConnections() {
	client.pool.reapIdleConnections()
}

type HealthCheckDaxAPI interface {
	startHealthChecks(cc *cluster, host hostPort)
}
