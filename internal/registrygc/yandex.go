package registrygc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	containerregistry "github.com/yandex-cloud/go-genproto/yandex/cloud/containerregistry/v1"
	operationpb "github.com/yandex-cloud/go-genproto/yandex/cloud/operation"
	containers "github.com/yandex-cloud/go-genproto/yandex/cloud/serverless/containers/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const (
	DefaultContainerRegistryEndpoint = "container-registry.api.cloud.yandex.net:443"
	DefaultServerlessEndpoint        = "serverless-containers.api.cloud.yandex.net:443"
	DefaultOperationEndpoint         = "operation.api.cloud.yandex.net:443"
	maxAPIPages                      = 1000
	postDeleteChecks                 = 30
)

type bearerToken string

func (token bearerToken) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(token)}, nil
}

func (bearerToken) RequireTransportSecurity() bool { return true }

type YandexCloud struct {
	containerConn *grpc.ClientConn
	registryConn  *grpc.ClientConn
	operationConn *grpc.ClientConn
	containers    containers.ContainerServiceClient
	repositories  containerregistry.RepositoryServiceClient
	images        containerregistry.ImageServiceClient
	operations    operationpb.OperationServiceClient
	pollInterval  time.Duration
}

type YandexConfig struct {
	Token                     string
	ContainerRegistryEndpoint string
	ServerlessEndpoint        string
	OperationEndpoint         string
}

func NewYandexCloud(config YandexConfig) (*YandexCloud, error) {
	if config.Token == "" || strings.TrimSpace(config.Token) != config.Token || strings.ContainsAny(config.Token, "\r\n") {
		return nil, errors.New("YC_TOKEN is required")
	}
	if config.ContainerRegistryEndpoint == "" {
		config.ContainerRegistryEndpoint = DefaultContainerRegistryEndpoint
	}
	if config.ServerlessEndpoint == "" {
		config.ServerlessEndpoint = DefaultServerlessEndpoint
	}
	if config.OperationEndpoint == "" {
		config.OperationEndpoint = DefaultOperationEndpoint
	}
	dial := func(endpoint string) (*grpc.ClientConn, error) {
		return grpc.NewClient(endpoint,
			grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})),
			grpc.WithPerRPCCredentials(bearerToken(config.Token)))
	}
	containerConn, err := dial(config.ServerlessEndpoint)
	if err != nil {
		return nil, err
	}
	registryConn, err := dial(config.ContainerRegistryEndpoint)
	if err != nil {
		containerConn.Close()
		return nil, err
	}
	operationConn, err := dial(config.OperationEndpoint)
	if err != nil {
		containerConn.Close()
		registryConn.Close()
		return nil, err
	}
	return &YandexCloud{
		containerConn: containerConn, registryConn: registryConn, operationConn: operationConn,
		containers:   containers.NewContainerServiceClient(containerConn),
		repositories: containerregistry.NewRepositoryServiceClient(registryConn),
		images:       containerregistry.NewImageServiceClient(registryConn),
		operations:   operationpb.NewOperationServiceClient(operationConn),
		pollInterval: time.Second,
	}, nil
}

func (cloud *YandexCloud) Close() error {
	if cloud == nil {
		return nil
	}
	return errors.Join(cloud.containerConn.Close(), cloud.registryConn.Close(), cloud.operationConn.Close())
}

func (cloud *YandexCloud) Discover(ctx context.Context, inventory Inventory) (LiveState, error) {
	result := LiveState{
		Containers:   make(map[string]LiveContainer),
		Repositories: make(map[string]LiveRepository),
	}
	for _, expected := range inventory.Containers {
		container, err := cloud.containers.Get(ctx,
			&containers.GetContainerRequest{ContainerId: expected.ContainerID})
		if err != nil {
			return LiveState{}, err
		}
		revisions, listErr := cloud.listRevisions(ctx, container.GetId())
		if listErr != nil {
			return LiveState{}, fmt.Errorf("list revisions for %s: %w", container.GetId(), listErr)
		}
		result.Containers[container.GetId()] = LiveContainer{
			ID: container.GetId(), Name: container.GetName(), Labels: container.GetLabels(), Revisions: revisions,
		}
	}
	for _, component := range RequiredComponents {
		expected := inventory.Repositories[component]
		repository, err := cloud.repositories.GetByName(ctx,
			&containerregistry.GetRepositoryByNameRequest{RepositoryName: expected.Name})
		if err != nil {
			return LiveState{}, fmt.Errorf("get repository %s: %w", component, err)
		}
		images, err := cloud.listImages(ctx, expected.Name)
		if err != nil {
			return LiveState{}, fmt.Errorf("list images for %s: %w", component, err)
		}
		result.Repositories[component] = LiveRepository{
			ID: repository.GetId(), Name: repository.GetName(), Images: images,
		}
	}
	return result, nil
}

func (cloud *YandexCloud) listRevisions(ctx context.Context, containerID string) ([]LiveRevision, error) {
	var revisions []LiveRevision
	pageToken := ""
	seenTokens := make(map[string]struct{})
	for pageNumber := 0; pageNumber < maxAPIPages; pageNumber++ {
		request := &containers.ListContainersRevisionsRequest{PageSize: 1000, PageToken: pageToken}
		request.SetContainerId(containerID)
		page, err := cloud.containers.ListRevisions(ctx, request)
		if err != nil {
			return nil, err
		}
		for _, revision := range page.GetRevisions() {
			createdAt, err := validTime(revision.GetCreatedAt())
			if err != nil {
				return nil, fmt.Errorf("revision %s created_at: %w", revision.GetId(), err)
			}
			revisions = append(revisions, LiveRevision{
				ID: revision.GetId(), Status: revision.GetStatus().String(), CreatedAt: createdAt,
				ImageURL: revision.GetImage().GetImageUrl(), ImageDigest: revision.GetImage().GetImageDigest(),
			})
		}
		next := page.GetNextPageToken()
		if next == "" {
			return revisions, nil
		}
		if _, duplicate := seenTokens[next]; duplicate {
			return nil, errors.New("revision pagination repeated a page token")
		}
		seenTokens[next] = struct{}{}
		pageToken = next
	}
	return nil, fmt.Errorf("revision pagination exceeded %d pages", maxAPIPages)
}

func (cloud *YandexCloud) listImages(ctx context.Context, repositoryName string) ([]RegistryImage, error) {
	var images []RegistryImage
	pageToken := ""
	seenTokens := make(map[string]struct{})
	for pageNumber := 0; pageNumber < maxAPIPages; pageNumber++ {
		page, err := cloud.images.List(ctx, &containerregistry.ListImagesRequest{
			RepositoryName: repositoryName, PageSize: 1000, PageToken: pageToken,
		})
		if err != nil {
			return nil, err
		}
		for _, image := range page.GetImages() {
			createdAt, err := validTime(image.GetCreatedAt())
			if err != nil {
				return nil, fmt.Errorf("image %s created_at: %w", image.GetId(), err)
			}
			images = append(images, RegistryImage{
				ID: image.GetId(), Digest: image.GetDigest(), CreatedAt: createdAt,
				CompressedSize: image.GetCompressedSize(), Tags: append([]string(nil), image.GetTags()...),
			})
		}
		next := page.GetNextPageToken()
		if next == "" {
			return images, nil
		}
		if _, duplicate := seenTokens[next]; duplicate {
			return nil, errors.New("image pagination repeated a page token")
		}
		seenTokens[next] = struct{}{}
		pageToken = next
	}
	return nil, fmt.Errorf("image pagination exceeded %d pages", maxAPIPages)
}

func (cloud *YandexCloud) GetImage(ctx context.Context, imageID string) (CloudImage, error) {
	image, err := cloud.images.Get(ctx, &containerregistry.GetImageRequest{ImageId: imageID})
	if status.Code(err) == codes.NotFound {
		return CloudImage{}, ErrImageNotFound
	}
	if err != nil {
		return CloudImage{}, err
	}
	createdAt, err := validTime(image.GetCreatedAt())
	if err != nil {
		return CloudImage{}, err
	}
	return CloudImage{ID: image.GetId(), Name: image.GetName(), Digest: image.GetDigest(), CreatedAt: createdAt}, nil
}

func (cloud *YandexCloud) DeleteImage(ctx context.Context, imageID string) error {
	operation, err := cloud.images.Delete(ctx, &containerregistry.DeleteImageRequest{ImageId: imageID})
	if status.Code(err) == codes.NotFound {
		return ErrImageNotFound
	}
	if err != nil {
		return err
	}
	for {
		if operation.GetDone() {
			if operationError := operation.GetError(); operationError != nil {
				return status.Error(codes.Code(operationError.GetCode()), operationError.GetMessage())
			}
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(cloud.pollInterval):
		}
		operation, err = cloud.operations.Get(ctx,
			&operationpb.GetOperationRequest{OperationId: operation.GetId()})
		if err != nil {
			return err
		}
	}
	for check := 0; check < postDeleteChecks; check++ {
		_, err := cloud.images.Get(ctx, &containerregistry.GetImageRequest{ImageId: imageID})
		if status.Code(err) == codes.NotFound {
			return nil
		}
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(cloud.pollInterval):
		}
	}
	return fmt.Errorf("image %s still exists after completed delete operation", imageID)
}

type validTimestamp interface {
	CheckValid() error
	AsTime() time.Time
}

func validTime(timestamp validTimestamp) (time.Time, error) {
	if timestamp == nil {
		return time.Time{}, errors.New("timestamp is missing")
	}
	if err := timestamp.CheckValid(); err != nil {
		return time.Time{}, err
	}
	return timestamp.AsTime().UTC(), nil
}
