package s3

import (
	"context"
	"fmt"
	"sort"
	"strings"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type Client struct {
	config Config
}

type GetIngestrURI interface {
	GetIngestrURI() string
}

func NewClient(c Config) (*Client, error) {
	return &Client{
		config: c,
	}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}

func (c *Client) ListBuckets(ctx context.Context) ([]string, error) {
	client, err := c.newSDKClient(ctx, "")
	if err != nil {
		return nil, err
	}

	result, err := client.ListBuckets(ctx, &awss3.ListBucketsInput{})
	if err != nil {
		return nil, fmt.Errorf("failed to list S3 buckets: %w", err)
	}

	buckets := make([]string, 0, len(result.Buckets))
	for _, bucket := range result.Buckets {
		if bucket.Name == nil || strings.TrimSpace(*bucket.Name) == "" {
			continue
		}
		buckets = append(buckets, strings.TrimSpace(*bucket.Name))
	}

	sort.Strings(buckets)
	return buckets, nil
}

func (c *Client) ListEntries(ctx context.Context, bucketName, prefix string) ([]string, error) {
	bucketName = strings.TrimSpace(bucketName)
	if bucketName == "" {
		return nil, fmt.Errorf("bucket name is required")
	}

	client, err := c.newSDKClient(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	cleanPrefix := strings.TrimSpace(prefix)
	delimiter := "/"
	maxKeys := int32(1000)
	result, err := client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket:    &bucketName,
		Prefix:    &cleanPrefix,
		Delimiter: &delimiter,
		MaxKeys:   &maxKeys,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list S3 objects for bucket '%s': %w", bucketName, err)
	}

	entries := make([]string, 0, len(result.CommonPrefixes)+len(result.Contents))
	seen := make(map[string]struct{}, cap(entries))

	for _, commonPrefix := range result.CommonPrefixes {
		if commonPrefix.Prefix == nil {
			continue
		}
		value := strings.TrimSpace(*commonPrefix.Prefix)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		entries = append(entries, value)
	}

	for _, object := range result.Contents {
		if object.Key == nil {
			continue
		}
		value := strings.TrimSpace(*object.Key)
		if value == "" || value == cleanPrefix {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		entries = append(entries, value)
	}

	sort.Strings(entries)
	return entries, nil
}

func (c *Client) newSDKClient(ctx context.Context, bucketName string) (*awss3.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			c.config.AccessKeyID,
			c.config.SecretAccessKey,
			"",
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	region := "us-east-1"
	if strings.TrimSpace(c.config.EndpointURL) != "" {
		cfg.Region = region
		endpointURL := strings.TrimSpace(c.config.EndpointURL)
		return awss3.NewFromConfig(cfg, func(options *awss3.Options) {
			options.BaseEndpoint = &endpointURL
			options.UsePathStyle = true
		}), nil
	}

	if strings.TrimSpace(bucketName) != "" {
		tmpCfg := cfg
		tmpCfg.Region = region
		tmpClient := awss3.NewFromConfig(tmpCfg)
		discoveredRegion, err := manager.GetBucketRegion(ctx, tmpClient, bucketName)
		if err == nil && strings.TrimSpace(discoveredRegion) != "" {
			region = discoveredRegion
		}
	}

	cfg.Region = region
	return awss3.NewFromConfig(cfg), nil
}
