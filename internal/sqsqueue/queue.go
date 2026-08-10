// Package sqsqueue implements the at-least-once queue port over SQS-compatible
// services. Local development uses ElasticMQ; cloud deployments use YMQ.
package sqsqueue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
)

var ErrNoMessage = errors.New("queue contains no visible message")

type Config struct {
	Endpoint        string
	Region          string
	QueueURL        string
	DeadLetterURL   string
	AccessKeyID     string
	SecretAccessKey string
	WaitTime        time.Duration
}

type inflightMessage struct {
	body string
}

type Queue struct {
	client        *sqs.Client
	queueURL      string
	deadLetterURL string
	waitSeconds   int32

	mu       sync.Mutex
	inflight map[string]inflightMessage
}

func New(ctx context.Context, config Config) (*Queue, error) {
	if strings.TrimSpace(config.Region) == "" {
		return nil, fmt.Errorf("queue region is required")
	}
	if strings.TrimSpace(config.QueueURL) == "" {
		return nil, fmt.Errorf("queue URL is required")
	}
	if (config.AccessKeyID == "") != (config.SecretAccessKey == "") {
		return nil, fmt.Errorf("queue access key and secret must be supplied together")
	}
	if config.WaitTime < 0 || config.WaitTime > 20*time.Second {
		return nil, fmt.Errorf("queue wait time must be between 0 and 20 seconds")
	}

	loadOptions := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(config.Region),
	}
	if config.AccessKeyID != "" {
		loadOptions = append(loadOptions, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(config.AccessKeyID, config.SecretAccessKey, ""),
		))
	}
	awsConfig, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load queue configuration: %w", err)
	}

	client := sqs.NewFromConfig(awsConfig, func(options *sqs.Options) {
		if config.Endpoint != "" {
			options.BaseEndpoint = aws.String(config.Endpoint)
		}
	})
	return &Queue{
		client:        client,
		queueURL:      config.QueueURL,
		deadLetterURL: config.DeadLetterURL,
		waitSeconds:   int32(config.WaitTime / time.Second),
		inflight:      make(map[string]inflightMessage),
	}, nil
}

func (queue *Queue) Publish(ctx context.Context, envelope queuecontract.Envelope) error {
	body, err := queuecontract.Encode(envelope)
	if err != nil {
		return err
	}
	_, err = queue.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queue.queueURL),
		MessageBody: aws.String(string(body)),
	})
	if err != nil {
		return fmt.Errorf("publish queue message: %w", err)
	}
	return nil
}

func (queue *Queue) Receive(ctx context.Context) (ports.ReceivedMessage, error) {
	result, err := queue.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queue.queueURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     queue.waitSeconds,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameApproximateReceiveCount,
		},
	})
	if err != nil {
		return ports.ReceivedMessage{}, fmt.Errorf("receive queue message: %w", err)
	}
	if len(result.Messages) == 0 {
		return ports.ReceivedMessage{}, ErrNoMessage
	}

	message := result.Messages[0]
	if message.Body == nil || message.ReceiptHandle == nil {
		return ports.ReceivedMessage{}, fmt.Errorf("received queue message lacks body or receipt handle")
	}
	deliveryCount := uint32(1)
	if value := message.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)]; value != "" {
		parsed, parseErr := strconv.ParseUint(value, 10, 32)
		if parseErr != nil {
			return ports.ReceivedMessage{}, fmt.Errorf("parse queue delivery count: %w", parseErr)
		}
		deliveryCount = uint32(parsed)
	}

	envelope, err := queuecontract.Decode([]byte(*message.Body))
	if err != nil {
		return ports.ReceivedMessage{}, fmt.Errorf("decode received queue message: %w", err)
	}
	queue.mu.Lock()
	queue.inflight[*message.ReceiptHandle] = inflightMessage{body: *message.Body}
	queue.mu.Unlock()

	return ports.ReceivedMessage{
		Envelope:      envelope,
		ReceiptHandle: *message.ReceiptHandle,
		DeliveryCount: deliveryCount,
	}, nil
}

func (queue *Queue) Ack(ctx context.Context, receiptHandle string) error {
	if strings.TrimSpace(receiptHandle) == "" {
		return fmt.Errorf("receipt handle is required")
	}
	_, err := queue.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queue.queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	if err != nil {
		return fmt.Errorf("ack queue message: %w", err)
	}
	queue.forget(receiptHandle)
	return nil
}

func (queue *Queue) Retry(ctx context.Context, receiptHandle string, delay time.Duration) error {
	if strings.TrimSpace(receiptHandle) == "" {
		return fmt.Errorf("receipt handle is required")
	}
	if delay < 0 || delay > 12*time.Hour {
		return fmt.Errorf("retry delay must be between 0 and 12 hours")
	}
	_, err := queue.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(queue.queueURL),
		ReceiptHandle:     aws.String(receiptHandle),
		VisibilityTimeout: retryVisibilitySeconds(delay),
	})
	if err != nil {
		return fmt.Errorf("retry queue message: %w", err)
	}
	queue.forget(receiptHandle)
	return nil
}

func retryVisibilitySeconds(delay time.Duration) int32 {
	if delay <= 0 {
		return 0
	}
	return int32((delay + time.Second - 1) / time.Second)
}

func (queue *Queue) DeadLetter(ctx context.Context, receiptHandle, reasonCode string) error {
	if strings.TrimSpace(receiptHandle) == "" {
		return fmt.Errorf("receipt handle is required")
	}
	if strings.TrimSpace(queue.deadLetterURL) == "" {
		return fmt.Errorf("dead-letter queue URL is required")
	}
	if strings.TrimSpace(reasonCode) == "" {
		return fmt.Errorf("dead-letter reason code is required")
	}

	queue.mu.Lock()
	message, ok := queue.inflight[receiptHandle]
	queue.mu.Unlock()
	if !ok {
		return fmt.Errorf("receipt handle is not tracked as inflight")
	}

	_, err := queue.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queue.deadLetterURL),
		MessageBody: aws.String(message.body),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"reason_code": {
				DataType:    aws.String("String"),
				StringValue: aws.String(reasonCode),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("publish dead-letter message: %w", err)
	}
	if err := queue.Ack(ctx, receiptHandle); err != nil {
		return fmt.Errorf("remove dead-lettered source message: %w", err)
	}
	return nil
}

func (queue *Queue) forget(receiptHandle string) {
	queue.mu.Lock()
	delete(queue.inflight, receiptHandle)
	queue.mu.Unlock()
}

var _ ports.Queue = (*Queue)(nil)
