package cmd

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/totoview/xin/core"
	log "github.com/totoview/xin/log"
	"github.com/totoview/xin/service/mgw"
	"github.com/totoview/xin/service/mgwapi"
	"github.com/totoview/xin/service/query"
	"github.com/totoview/xin/service/store"
	"go.uber.org/zap"
)

func createService(name string, globalLoggingConfig *viper.Viper) (core.Service, error) {

	svcConfig := viper.Sub(name)
	logger, err := log.New(name, globalLoggingConfig, svcConfig.Sub("logging"))
	if err != nil {
		return nil, err
	}

	switch name {
	case "mgwapi":
		return mgwapi.NewFromConfig(svcConfig, logger)
	case "mgw":
		return mgw.NewFromConfig(svcConfig, logger)
	case "store":
		return store.NewFromConfig(svcConfig, logger)
	case "query":
		return query.NewFromConfig(svcConfig, logger)
	}
	return nil, errors.Errorf("Unknows service %s", name)
}

func runServices(names ...string) {

	// load config
	if err := viper.ReadInConfig(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to load configuration: %v\n", err)
		return
	}

	globalLoggingConfig := viper.Sub("logging")
	globalLogger, err := log.New("", globalLoggingConfig, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logger: %v\n", err)
		return
	}

	globalLogger.Info("Start app", zap.Int("pid", os.Getpid()))

	var services []core.Service
	for _, name := range names {
		svc, err := createService(name, globalLoggingConfig)
		if err != nil {
			globalLogger.Error(fmt.Sprintf("Failed to create service %s", name), zap.Error(err))
			return
		}
		services = append(services, svc)
	}

	for _, svc := range services {
		if err := svc.Init(1000); err != nil {
			return
		}
	}

	for _, svc := range services {
		if err := svc.Start(1000); err != nil {
			return
		}
	}

	errc := make(chan error)

	// interrupt
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		errc <- fmt.Errorf("%s", <-c)
	}()

	err = <-errc
	globalLogger.Info("Stop app", zap.Error(err))

	// exit, allow enough time for cleanup
	for i := len(services) - 1; i >= 0; i-- {
		services[i].Stop(10000)
	}
}
