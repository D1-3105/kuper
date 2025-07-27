package driver

import (
	"context"
	"fmt"
	"net"
	"os"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
)

const (
	driverName    = "torrent.csi.driver"
	driverVersion = "0.1.0"
	socketPath    = "/var/run/csi/csi.sock"
)

type TorrentCSIDriver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer
}

func NewMyCSIDriver() *TorrentCSIDriver {
	return &TorrentCSIDriver{}
}

func (d *TorrentCSIDriver) Run() error {
	_ = os.Remove(socketPath)

	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s := grpc.NewServer()
	csi.RegisterIdentityServer(s, d)
	csi.RegisterControllerServer(s, d)
	csi.RegisterNodeServer(s, d)

	fmt.Println("Starting CSI driver at", socketPath)
	return s.Serve(lis)
}

// --- Identity Server ---
func (d *TorrentCSIDriver) GetPluginInfo(
	ctx context.Context,
	req *csi.GetPluginInfoRequest,
) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          driverName,
		VendorVersion: driverVersion,
	}, nil
}

func (d *TorrentCSIDriver) GetPluginCapabilities(
	ctx context.Context,
	req *csi.GetPluginCapabilitiesRequest,
) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{}, nil
}

func (d *TorrentCSIDriver) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}

// --- Controller Server ---
func (d *TorrentCSIDriver) CreateVolume(
	ctx context.Context,
	req *csi.CreateVolumeRequest,
) (*csi.CreateVolumeResponse, error) {
	return nil, fmt.Errorf("CreateVolume not implemented")
}

func (d *TorrentCSIDriver) DeleteVolume(
	ctx context.Context,
	req *csi.DeleteVolumeRequest,
) (*csi.DeleteVolumeResponse, error) {
	return nil, fmt.Errorf("DeleteVolume not implemented")
}

// --- Node Server ---
func (d *TorrentCSIDriver) NodePublishVolume(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest,
) (*csi.NodePublishVolumeResponse, error) {
	return nil, fmt.Errorf("NodePublishVolume not implemented")
}

func (d *TorrentCSIDriver) NodeUnpublishVolume(
	ctx context.Context,
	req *csi.NodeUnpublishVolumeRequest,
) (*csi.NodeUnpublishVolumeResponse, error) {
	return nil, fmt.Errorf("NodeUnpublishVolume not implemented")
}
