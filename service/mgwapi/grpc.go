package mgwapi

// GRPC public service

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/totoview/xin/core"
	pb "github.com/totoview/xin/pb/mgw"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type grpcService struct {
	logger *zap.Logger
	svc    Service
	config *core.InterfaceConfig
	server *grpc.Server
	done   chan struct{}
}

func newGRPCService(config *core.InterfaceConfig, svc Service, logger *zap.Logger) (*grpcService, error) {
	return &grpcService{svc: svc, config: config, logger: logger.With(zap.String("intf", "grpc"))}, nil
}

///////////////////////////////////////////////////////////////////////////////////////
// core.Service

func (s *grpcService) Init(tmoutMillis int64) error {
	s.done = make(chan struct{})
	return nil
}

func (s *grpcService) Start(tmoutMillis int64) error {
	lis, err := net.Listen("tcp", s.config.Listen)
	if err != nil {
		return err
	}

	var opts []grpc.ServerOption
	if s.config.Secure {
		creds, err := credentials.NewServerTLSFromFile(s.config.Cert, s.config.Key)
		if err != nil {
			return err
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}

	s.server = grpc.NewServer(opts...)
	pb.RegisterMGWServer(s.server, s)
	errc := make(chan error, 1)
	go func() {
		errc <- s.server.Serve(lis)
		s.done <- struct{}{}
	}()

	select {
	case err := <-errc:
		return err
	case <-time.After(5 * time.Millisecond):
		s.logger.Info("Start GRPC", zap.String("addr", s.config.Listen))
		return nil
	}
}

func (s *grpcService) Stop(tmoutMillis int64) error {
	// this also closes the listener:
	s.server.GracefulStop()
	<-s.done
	s.logger.Info("Stop GRPC", zap.String("addr", s.config.Listen))
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// pb.MGWServer

func (s *grpcService) Send(svr pb.MGW_SendServer) error {
	ctx := context.Background()
	for {
		req, err := svr.Recv()
		if err == io.EOF {
			return nil
		}
		rsp, err := s.svc.Send(ctx, []*pb.MsgReq{req})
		if err != nil {
			return err
		}
		if err := svr.Send(rsp[0]); err != nil {
			return err
		}
	}
}
