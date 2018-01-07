package mgwapi

// REST public service

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-kit/kit/endpoint"
	httptransport "github.com/go-kit/kit/transport/http"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/totoview/xin/core"
	"github.com/totoview/xin/log"
	pb "github.com/totoview/xin/pb/mgw"
	"go.uber.org/zap"
)

type restService struct {
	logger *zap.Logger
	svc    Service
	config *core.InterfaceConfig
	server *http.Server
	done   chan struct{}
}

func newRESTService(config *core.InterfaceConfig, svc Service, logger *zap.Logger) (*restService, error) {
	return &restService{svc: svc, config: config, logger: logger.With(zap.String("intf", "rest")), done: make(chan struct{})}, nil
}

func encodeError(svc *restService) func(context.Context, error, http.ResponseWriter) {
	return func(_ context.Context, err error, w http.ResponseWriter) {
		if err == nil {
			svc.logger.DPanic("encodeError with nil error")
			err = errors.Errorf("")
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": err.Error(),
		})
	}
}

// Send

func makeSendEndpoint(svc Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		return svc.Send(ctx, request.([]*pb.MsgReq))
	}
}

func decodeSendRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var requests []*pb.MsgReq
	if err := json.NewDecoder(r.Body).Decode(&requests); err != nil {
		return nil, err
	}
	return requests, nil
}

func encodeSendResponse(_ context.Context, w http.ResponseWriter, response interface{}) error {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	return json.NewEncoder(w).Encode(response)
}

/////////////////////////////////////////////////////////////////////////////////////
// core.Service

func (s *restService) Init(tmoutMillis int64) error {
	return nil
}

func (s *restService) Start(tmoutMillis int64) error {
	options := []httptransport.ServerOption{
		httptransport.ServerErrorLogger(log.NewGoKitLogger("http", s.logger)),
		httptransport.ServerErrorEncoder(encodeError(s)),
	}

	r := mux.NewRouter()

	// send messages
	r.Methods("POST").Path("/message").Handler(httptransport.NewServer(
		makeSendEndpoint(s.svc),
		decodeSendRequest,
		encodeSendResponse,
		options...,
	))

	s.server = &http.Server{Addr: s.config.Listen, Handler: r}
	errc := make(chan error, 1)

	go func() {
		if s.config.Secure {
			errc <- s.server.ListenAndServeTLS(s.config.Cert, s.config.Key)
		} else {
			errc <- s.server.ListenAndServe()
		}
		s.done <- struct{}{}
	}()

	select {
	case err := <-errc:
		return err
	case <-time.After(5 * time.Millisecond):
		s.logger.Info("Start REST", zap.String("addr", s.config.Listen))
		return nil
	}
}

func (s *restService) Stop(tmoutMillis int64) error {
	err := s.server.Shutdown(context.Background())
	<-s.done
	s.logger.Info("Stop REST", zap.String("addr", s.config.Listen))
	return err
}
