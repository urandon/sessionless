package registrygc

import (
	"context"
	"strings"
	"testing"
	"time"

	containerregistry "github.com/yandex-cloud/go-genproto/yandex/cloud/containerregistry/v1"
	containers "github.com/yandex-cloud/go-genproto/yandex/cloud/serverless/containers/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type pagedImageClient struct {
	containerregistry.ImageServiceClient
	pages map[string]*containerregistry.ListImagesResponse
	seen  []string
}

func (client *pagedImageClient) List(
	_ context.Context,
	request *containerregistry.ListImagesRequest,
	_ ...grpc.CallOption,
) (*containerregistry.ListImagesResponse, error) {
	client.seen = append(client.seen, request.GetPageToken())
	return client.pages[request.GetPageToken()], nil
}

type pagedContainerClient struct {
	containers.ContainerServiceClient
	pages map[string]*containers.ListContainersRevisionsResponse
	seen  []string
}

func (client *pagedContainerClient) ListRevisions(
	_ context.Context,
	request *containers.ListContainersRevisionsRequest,
	_ ...grpc.CallOption,
) (*containers.ListContainersRevisionsResponse, error) {
	client.seen = append(client.seen, request.GetPageToken())
	return client.pages[request.GetPageToken()], nil
}

func TestYandexListImagesDrainsEveryPage(t *testing.T) {
	createdAt := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	client := &pagedImageClient{pages: map[string]*containerregistry.ListImagesResponse{
		"": {
			Images: []*containerregistry.Image{{
				Id: "image1", Digest: digest(1), CreatedAt: timestamppb.New(createdAt),
				CompressedSize: 101, Tags: []string{"first"},
			}},
			NextPageToken: "page2",
		},
		"page2": {
			Images: []*containerregistry.Image{{
				Id: "image2", Digest: digest(2), CreatedAt: timestamppb.New(createdAt.Add(time.Hour)),
				CompressedSize: 202, Tags: []string{"second"},
			}},
		},
	}}
	cloud := &YandexCloud{images: client}
	images, err := cloud.listImages(context.Background(), testRegistryID+"/control-api")
	if err != nil {
		t.Fatalf("listImages() error = %v", err)
	}
	if got, want := strings.Join(client.seen, ","), ",page2"; got != want {
		t.Fatalf("page tokens = %q, want %q", got, want)
	}
	if len(images) != 2 || images[0].ID != "image1" || images[1].ID != "image2" {
		t.Fatalf("images = %#v, want both pages in order", images)
	}
	if images[1].CompressedSize != 202 || !images[1].CreatedAt.Equal(createdAt.Add(time.Hour)) {
		t.Fatalf("second image was not mapped completely: %#v", images[1])
	}
}

func TestYandexListImagesRejectsRepeatedPageToken(t *testing.T) {
	client := &pagedImageClient{pages: map[string]*containerregistry.ListImagesResponse{
		"":     {NextPageToken: "loop"},
		"loop": {NextPageToken: "loop"},
	}}
	cloud := &YandexCloud{images: client}
	images, err := cloud.listImages(context.Background(), testRegistryID+"/control-api")
	if err == nil || !strings.Contains(err.Error(), "repeated a page token") {
		t.Fatalf("listImages() = %#v, %v; want repeated-token failure", images, err)
	}
	if images != nil {
		t.Fatalf("pagination failure returned partial images: %#v", images)
	}
}

func TestYandexListRevisionsDrainsEveryPage(t *testing.T) {
	createdAt := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	client := &pagedContainerClient{pages: map[string]*containers.ListContainersRevisionsResponse{
		"": {
			Revisions: []*containers.Revision{{
				Id: "revision1", Status: containers.Revision_ACTIVE, CreatedAt: timestamppb.New(createdAt),
				Image: &containers.Image{ImageUrl: imageReference("control-api", digest(1)), ImageDigest: digest(1)},
			}},
			NextPageToken: "page2",
		},
		"page2": {
			Revisions: []*containers.Revision{{
				Id: "revision2", Status: containers.Revision_OBSOLETE, CreatedAt: timestamppb.New(createdAt.Add(-time.Hour)),
				Image: &containers.Image{ImageUrl: imageReference("control-api", digest(2)), ImageDigest: digest(2)},
			}},
		},
	}}
	cloud := &YandexCloud{containers: client}
	revisions, err := cloud.listRevisions(context.Background(), "container1")
	if err != nil {
		t.Fatalf("listRevisions() error = %v", err)
	}
	if got, want := strings.Join(client.seen, ","), ",page2"; got != want {
		t.Fatalf("page tokens = %q, want %q", got, want)
	}
	if len(revisions) != 2 || revisions[0].Status != "ACTIVE" || revisions[1].Status != "OBSOLETE" {
		t.Fatalf("revisions = %#v, want both pages mapped", revisions)
	}
}

func TestYandexListRevisionsRejectsRepeatedPageToken(t *testing.T) {
	client := &pagedContainerClient{pages: map[string]*containers.ListContainersRevisionsResponse{
		"":     {NextPageToken: "loop"},
		"loop": {NextPageToken: "loop"},
	}}
	cloud := &YandexCloud{containers: client}
	revisions, err := cloud.listRevisions(context.Background(), "container1")
	if err == nil || !strings.Contains(err.Error(), "repeated a page token") {
		t.Fatalf("listRevisions() = %#v, %v; want repeated-token failure", revisions, err)
	}
	if revisions != nil {
		t.Fatalf("pagination failure returned partial revisions: %#v", revisions)
	}
}
