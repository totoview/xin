package log

import (
	"fmt"
	"os"

	glog "github.com/go-kit/kit/log"
	"github.com/spf13/viper"
	"github.com/totoview/xin/core"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// these options, if present, override all values specified through other means.
// they are meant to make it easy to change common logging options without editing
// configuration files during development
var (
	Level   string
	Verbose bool
)

var (
	defaultLevel   = core.LoggingLevelInfo
	defaultMode    = core.LoggingModeProduction
	defaultEncoder = core.LoggingEncoderConsole

	defaultOptions = &core.LoggingOptionsConfig{
		Level:           defaultLevel,
		Mode:            defaultMode,
		Encoder:         defaultEncoder,
		WithServiceName: true,
		WithCaller:      false,
	}
)

// New creates a logger based on global logging configuration global
// and service specific logging configuration svc. If no logger is
// created from the configuration, a default logger will be used.
func New(svcName string, global *viper.Viper, svc *viper.Viper) (*zap.Logger, error) {
	var errs error

	globalConfig, err := parseConfig(global)
	errs = core.WrapErr(errs, err)

	svcConfig, err := parseConfig(svc)
	errs = core.WrapErr(errs, err)

	// merge global options into app options
	if globalConfig != nil && svcConfig != nil {
		svcConfig.Options = mergeOptions(globalConfig.Options, svcConfig.Options)
	}

	configs := getLoggerConfigs(globalConfig)
	configs = append(configs, getLoggerConfigs(svcConfig)...)

	if len(configs) == 0 {
		fmt.Fprintf(os.Stderr, "*** WARN ***: unable to create logger, fall back to the default logger (%s)\n", errs.Error())
		configs = append(configs, &core.LoggerConfig{
			Type:    "file",
			Options: defaultOptions,
			File: &core.FileLoggerConfig{
				Path: "stdout",
			},
		})
	}
	var cores []zapcore.Core
	for _, cfg := range configs {
		if c, err := getCore(svcName, cfg); err == nil {
			cores = append(cores, c)
		} else {
			errs = core.WrapErr(errs, err)
		}
	}
	if len(cores) == 0 {
		return nil, errs
	}
	var finalCore zapcore.Core
	if len(cores) == 1 {
		finalCore = cores[0]
	} else {
		finalCore = zapcore.NewTee(cores...)
	}

	var options []zap.Option

	// options specified at logger level are not considered for with_caller since
	// this option only works at logger level in Zap
	if (globalConfig != nil && globalConfig.Options.WithCaller) || (svcConfig != nil && svcConfig.Options.WithCaller) {
		options = append(options, zap.AddCaller())
	}

	return zap.New(finalCore, options...), nil
}

func getCore(svcName string, config *core.LoggerConfig) (zapcore.Core, error) {
	// apply default options
	config.Options = mergeOptions(defaultOptions, config.Options)

	// apply command line options
	if Verbose {
		config.Options.Level = core.LoggingLevelDebug
	} else {
		switch Level {
		case core.LoggingLevelDebug:
			config.Options.Level = Level
		case core.LoggingLevelInfo:
			config.Options.Level = Level
		case core.LoggingLevelWarn:
			config.Options.Level = Level
		case core.LoggingLevelError:
			config.Options.Level = Level
		case core.LoggingLevelFatal:
			config.Options.Level = Level
		}
	}

	// determine final logging level based on config
	var level zapcore.Level

	switch config.Options.Level {
	case core.LoggingLevelDebug:
		level = zap.DebugLevel
	case core.LoggingLevelInfo:
		level = zap.InfoLevel
	case core.LoggingLevelWarn:
		level = zap.WarnLevel
	case core.LoggingLevelError:
		level = zap.ErrorLevel
	case core.LoggingLevelFatal:
		level = zap.FatalLevel
	}

	// encoder config
	var ec zapcore.EncoderConfig

	switch config.Options.Mode {
	case core.LoggingModeDevelopment:
		ec = zap.NewDevelopmentEncoderConfig()
	case core.LoggingModeProduction:
		ec = zap.NewProductionEncoderConfig()
	}

	switch config.Options.Encoder {
	case core.LoggingEncoderConsole:
		ec.EncodeLevel = zapcore.CapitalColorLevelEncoder
	case core.LoggingEncoderJSON:
		ec.EncodeLevel = zapcore.CapitalLevelEncoder
	}

	if config.Options.WithCaller {
		// ec.EncodeCaller = zapcore.ShortCallerEncoder
	}

	ec.EncodeTime = zapcore.ISO8601TimeEncoder

	// write
	var ws zapcore.WriteSyncer

	switch config.Type {
	default:
		return zapcore.NewNopCore(), nil
	case "file":
		switch config.File.Path {
		case "stdout":
			ws = zapcore.Lock(os.Stdout)
		case "stderr":
			ws = zapcore.Lock(os.Stderr)
		default:
			ec.EncodeLevel = zapcore.CapitalLevelEncoder // no color
			f, err := os.OpenFile(config.File.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return nil, err
			}
			ws = zapcore.AddSync(f)
		}
	}

	// encoder
	var enc zapcore.Encoder

	switch config.Options.Encoder {
	case core.LoggingEncoderConsole:
		enc = zapcore.NewConsoleEncoder(ec)
	case core.LoggingEncoderJSON:
		enc = zapcore.NewJSONEncoder(ec)
	}

	core := zapcore.NewCore(enc, ws, zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= level
	}))

	if svcName != "" && (config.Options.WithServiceName || Verbose) {
		core = core.With([]zapcore.Field{zap.String("svc", svcName)})
	}

	return core, nil
}

func parseConfig(config *viper.Viper) (*core.LoggingConfig, error) {
	if config == nil {
		return nil, nil
	}
	return core.GetLoggingConfig(config)
}

func getLoggerConfigs(config *core.LoggingConfig) []*core.LoggerConfig {
	if config == nil {
		return []*core.LoggerConfig{}
	}
	for _, logger := range config.Loggers {
		logger.Options = mergeOptions(config.Options, logger.Options)
	}
	return config.Loggers
}

func mergeOptions(base *core.LoggingOptionsConfig, config *core.LoggingOptionsConfig) *core.LoggingOptionsConfig {
	if base == nil {
		c := *config
		return &c
	}
	c := *base
	if config != nil {
		if config.Level != "" {
			c.Level = config.Level
		}
		if config.Mode != "" {
			c.Mode = config.Mode
		}
		if config.Encoder != "" {
			c.Encoder = config.Encoder
		}
	}
	return &c
}

type goKitLogger struct {
	defaultMsg string
	logger     *zap.SugaredLogger
}

// NewGoKitLogger creates a go-kit logger from a Zap. This is required by
// some go-kit native components. Since go-kit logger doesn't have the concept
// of log message, a default message is used to provide context of logging.
func NewGoKitLogger(defaultMsg string, logger *zap.Logger) glog.Logger {
	return &goKitLogger{defaultMsg: defaultMsg, logger: logger.Sugar()}
}

func (l *goKitLogger) Log(args ...interface{}) error {
	l.logger.Infow(l.defaultMsg, args...)
	return nil
}
