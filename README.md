# AWS DAX SDK for Go v2

aws-dax-go-v2 is the official AWS DAX SDK for the Go programming language. https://aws.amazon.com/dynamodb/dax

Checkout our [release notes](https://github.com/aws/aws-dax-go-v2/releases) for
information about the latest bug fixes, updates, and features added to the SDK.

## Getting started
The best way to get started working with the SDK is to use go get to add the SDK
to your Go Workspace manually.

    go get github.com/aws/aws-dax-go-v2

## Making API requests
This example shows how you can use the AWS DAX SDK to make an API request.

```go
package main

import (
    "context"
    "fmt"
    "net"

    "github.com/aws/aws-dax-go-v2/dax"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
    "github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
    "github.com/aws/aws-sdk-go-v2/aws"
)

func main() {
    ctx := context.Background()

    cfg := dax.DefaultConfig()
	cfg.HostPorts = []string{"dax://mycluster.frfx8h.clustercfg.dax.usw2.amazonaws.com:8111"}
	cfg.Region = "us-west-2"
	client, err := dax.New(cfg)

    if err != nil {
        panic(fmt.Errorf("unable to initialize client %v", err))
    }

    //Connecion to a secure cluster
    secureEndpoint := "daxs://mycluster.frfx8h.clustercfg.dax.usw2.amazonaws.com"
    secureCfg := dax.DefaultConfig()
	secureCfg.HostPorts = []string{secureEndpoint}
	secureCfg.Region = "us-west-2"
    
   //WARN: Skip hostname verification of TLS connections. 
	//The default is to perform hostname verification, setting this to True will skip verification. 
	//Be sure you understand the implication of doing so, which is the inability to authenticate
	//the cluster that you are connecting to.
    secureCfg.SkipHostnameVerification = false

   // DialContext is an optional field in Config.
	// If DialContext is being set in Config for a secure/ encrypted cluster, then use dax.SecureDialContext to 
	// return DialContext. An example of how DailContext can be set using dax.SecureDialContext is shown below.
    secureCfg.DialContext = func(ctx context.Context, network string, address string) (net.Conn, error) {
        // fmt.Println("Write your custom logic here")
        dialCon, err := dax.SecureDialContext(secureEndpoint, secureCfg.SkipHostnameVerification)
        if err != nil {
            panic(fmt.Errorf("secure dialcontext creation failed %v", err))
        }
        return dialCon(ctx, network, address)
    }
    secureClient, err := dax.New(secureCfg)
    if err != nil {
        panic(fmt.Errorf("unable to initialize secure client %v", err))
    }
    fmt.Println("secure client created", secureClient)

    // Marshal the values
    pkVal, err := attributevalue.Marshal("mykey")
    if err != nil {
        return fmt.Errorf("error marshaling pk value: %v", err)
    }

    skVal, err := attributevalue.Marshal("0")
    if err != nil {
        return fmt.Errorf("error marshaling sk value: %v", err)
    }

    valueVal, err := attributevalue.Marshal("myvalue")
    if err != nil {
        return fmt.Errorf("error marshaling value: %v", err)
    }

    // PutItem operation
    input := &dynamodb.PutItemInput{
        TableName: aws.String("TryDaxGoTable"),
        Item: map[string]types.AttributeValue{
            "pk":    pkVal,
            "sk":    skVal,
            "value": valueVal,
        },
    }

    output, err := client.PutItem(ctx, input)
    if err != nil {
        panic(fmt.Errorf("unable to make request %v", err))
    }

    fmt.Println("Output: ", output)

	// Scan with pagination example using DAX paginator
    scanInput := &dynamodb.ScanInput{
        TableName: aws.String("TryDaxGoTable"),
        Limit:     aws.Int32(10), // Items per page
    }

    // Create paginator
    paginator := dax.NewScanPaginator(client, scanInput)

    // Iterate through pages
    pageNum := 0
    for paginator.HasMorePages() {
        pageNum++
        fmt.Printf("Processing page %d\n", pageNum)

        // Get next page
        page, err := paginator.NextPage(ctx)
        if err != nil {
            panic(fmt.Errorf("failed to get page %d: %v", pageNum, err))
        }

        // Process items in the page
        for _, item := range page.Items {
            fmt.Printf("Item: %v\n", item)
        }
    }
}
```

## Feedback and contributing
**GitHub issues:** To provide feedback or report bugs, file GitHub
[Issues](https://github.com/aws/aws-dax-go-v2/issues) on the SDK.
This is the preferred mechanism to give feedback so that other users can engage in
the conversation, +1 issues, etc. Issues you open will be evaluated, and included
in our roadmap.

**Contributing:** You can open pull requests for fixes or additions to the
AWS DAX SDK for Go v2. All pull requests must be submitted under the Apache 2.0
license and will be reviewed by an SDK team member before being merged in.
Accompanying unit tests, where possible, are appreciated.

## License

This library is licensed under the Apache 2.0 License. 
