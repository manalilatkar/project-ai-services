package podman

import (
	"context"
	"fmt"

	"github.com/containers/podman/v5/pkg/bindings"
	"github.com/containers/podman/v5/pkg/bindings/images"
	"github.com/containers/podman/v5/pkg/bindings/pods"
)

type PodmanClient struct {
	Context context.Context
}

// NewPodmanClient creates and returns a new PodmanClient instance
func NewPodmanClient() (*PodmanClient, error) {
	ctx, err := bindings.NewConnectionWithIdentity(context.Background(), "ssh://root@127.0.0.1:51065/run/podman/podman.sock", "/Users/mayukac/.local/share/containers/podman/machine/machine", false)
	if err != nil {
		return nil, err
	}
	return &PodmanClient{Context: ctx}, nil
}

// Example function to list images (you can expand with more Podman functionalities)
func (pc *PodmanClient) ListImages() ([]string, error) {
	imagesList, err := images.List(pc.Context, nil)
	if err != nil {
		return nil, err
	}

	var imageNames []string
	for _, img := range imagesList {
		imageNames = append(imageNames, img.ID)
	}
	return imageNames, nil
}

func (pc *PodmanClient) ListPods() (any, error) {
	podList, err := pods.List(pc.Context, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}

	return podList, nil
}
