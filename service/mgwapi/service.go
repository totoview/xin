package mgwapi

import (
	"context"
	"encoding/json"
	"time"

	"github.com/boltdb/bolt"
	"github.com/gogo/protobuf/proto"
	"github.com/spf13/viper"
	"github.com/totoview/xin/core"
	"github.com/totoview/xin/kafka"
	base "github.com/totoview/xin/pb"
	pb "github.com/totoview/xin/pb/mgw"
	"go.uber.org/zap"
)

// dispatch message based on sender
func toKey(from *base.MsgAddr) []byte {
	key, _ := proto.Marshal(from)
	return key
}

// Service defines message gateway's public service interface.
type Service interface {
	core.Service
	Send(ctx context.Context, req []*pb.MsgReq) ([]*pb.MsgRsp, error)
}

// implementation
type service struct {
	logger       *zap.Logger
	interfaces   []core.Service
	updateConfig *core.ProducerConfig
	pipeline     *core.Pipeline
	reqc         chan<- []*kafka.ProducerMsg
}

// NewFromConfig makes a new message gateway API service based on config.
func NewFromConfig(config *viper.Viper, logger *zap.Logger) (core.Service, error) {
	if err := core.VerifyConfig(config, Schema); err != nil {
		return nil, err
	}
	var (
		sessionConfig, _ = core.GetSessionConfig(config.Sub("session"))
		updateConfig, _  = core.GetProducerConfig(config.Sub("update"))
	)

	db, err := bolt.Open(sessionConfig.Store.Bolt.Path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	ic, _ := core.GetInterfacesConfig(config.Get("interfaces").([]interface{}))
	svc, err := NewService(updateConfig, db, logger)
	if err != nil {
		return nil, err
	}

	for _, i := range ic.Interfaces {
		switch i.Type {
		case "rest":
			s, err := newRESTService(i, svc, logger)
			if err != nil {
				return nil, err
			}
			svc.interfaces = append(svc.interfaces, s)
		case "grpc":
			s, err := newGRPCService(i, svc, logger)
			if err != nil {
				return nil, err
			}
			svc.interfaces = append(svc.interfaces, s)
		}
	}

	return svc, nil
}

func NewService(updateConfig *core.ProducerConfig, db *bolt.DB, logger *zap.Logger) (*service, error) {
	pipeline := core.NewPipeline(updateConfig.Kafka.Topic, logger)

	// add event producer service
	ep, err := kafka.NewProducerNode(updateConfig, db, logger)
	if err != nil {
		return nil, err
	}
	pipeline.AddService(ep)
	return &service{updateConfig: updateConfig, pipeline: pipeline, logger: logger, reqc: ep.Input()}, nil
}

func (s *service) Send(ctx context.Context, reqs []*pb.MsgReq) ([]*pb.MsgRsp, error) {
	rsps := make([]*pb.MsgRsp, len(reqs))
	msgs := make([]*kafka.ProducerMsg, len(reqs))
	for i, req := range reqs {

		if ce := s.logger.Check(zap.DebugLevel, "New request"); ce != nil {
			data, _ := json.Marshal(req)
			ce.Write(zap.String("req", string(data)))
		}

		rsps[i] = &pb.MsgRsp{Id: req.Id, App: req.App, Session: req.Session}
		r := pb.NewRequestWithMsgReq(req)
		value, _ := proto.Marshal(&r)
		msgs[i] = &kafka.ProducerMsg{Topic: s.updateConfig.Kafka.Topic, Key: toKey(req.From), Value: value}
	}
	s.reqc <- msgs // can this block ? should we move this to a goroutine ?
	return rsps, nil
}

//////////////////////////////////////////////////////////////////////////////
// core.Service

func (s *service) Init(tmoutMillis int64) error {
	var err error
	err = core.WrapErr(err, s.pipeline.Init(tmoutMillis))
	for _, i := range s.interfaces {
		err = core.WrapErr(err, i.Init(tmoutMillis))
	}
	return err
}

func (s *service) Start(tmoutMillis int64) error {
	var err error
	err = core.WrapErr(err, s.pipeline.Start(tmoutMillis))
	for _, i := range s.interfaces {
		err = core.WrapErr(err, i.Start(tmoutMillis))
	}
	return err
}

func (s *service) Stop(tmoutMillis int64) error {
	var err error
	err = core.WrapErr(err, s.pipeline.Stop(tmoutMillis))
	for _, i := range s.interfaces {
		err = core.WrapErr(err, i.Stop(tmoutMillis))
	}
	return err
}
