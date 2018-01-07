package core

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/spf13/viper"

	"github.com/pkg/errors"
	"github.com/xeipuuv/gojsonschema"
)

// Common configuration schemas shared by various services
//
// Use map[string]interface{} to build up the schema, which is quite verbose.
// Another option is to use a literal JSON string. JSON string will be shorter
// but error-prone and hard to debug.

// Conf is an object
type Conf map[string]interface{}

// Enum is enum for strings
type Enum []string

const (
	LoggingLevelDebug = "debug"
	LoggingLevelInfo  = "info"
	LoggingLevelWarn  = "warn"
	LoggingLevelError = "error"
	LoggingLevelFatal = "fatal"

	LoggingModeDevelopment = "development"
	LoggingModeProduction  = "production"

	LoggingEncoderConsole = "console"
	LoggingEncoderJSON    = "json"
)

// LoggingOptionsSchema is schema for logging options
var LoggingOptionsSchema = Conf{
	"type":                 "object",
	"additionalProperties": false,
	"properties": Conf{
		"level":             Conf{"enum": Enum{LoggingLevelDebug, LoggingLevelInfo, LoggingLevelWarn, LoggingLevelError, LoggingLevelFatal}},
		"mode":              Conf{"enum": Enum{"production", "development"}},
		"encoder":           Conf{"enum": Enum{LoggingEncoderConsole, LoggingEncoderJSON}},
		"with_service_name": Conf{"type": "boolean"},
		"with_caller":       Conf{"type": "boolean"},
	},
}

type LoggingOptionsConfig struct {
	Level           string
	Mode            string
	Encoder         string
	WithServiceName bool
	WithCaller      bool
}

func GetLoggingOptionsConfig(config *viper.Viper) (*LoggingOptionsConfig, error) {
	if err := VerifyConfig(config, LoggingOptionsSchema); err != nil {
		return nil, err
	}
	config.SetDefault("withServiceName", true)

	opt := &LoggingOptionsConfig{
		Level:           config.GetString("level"),
		Mode:            config.GetString("mode"),
		Encoder:         config.GetString("encoder"),
		WithServiceName: config.GetBool("with_service_name"),
		WithCaller:      config.GetBool("with_caller"),
	}
	return opt, nil
}

// FileLoggerSchema is schema for file logger configuration
var FileLoggerSchema = Conf{
	"type":                 "object",
	"additionalProperties": false,
	"required":             Enum{"path"},
	"properties": Conf{
		"path": Conf{"type": "string"},
	},
}

type FileLoggerConfig struct {
	Path string
}

func GetFileLoggerConfig(config *viper.Viper) (*FileLoggerConfig, error) {
	if err := VerifyConfig(config, FileLoggerSchema); err != nil {
		return nil, err
	}
	return &FileLoggerConfig{
		Path: config.GetString("path"),
	}, nil
}

// LoggerSchema is schema for logger configuration
var LoggerSchema = Conf{
	"type":                 "object",
	"additionalProperties": false,
	"required":             Enum{"type"},
	"properties": Conf{
		"type":    Conf{"enum": Enum{"file"}},
		"options": LoggingOptionsSchema,
		"file":    FileLoggerSchema,
	},
	"allOf": []Conf{
		Conf{
			"anyOf": []Conf{
				// must have "file" when "type" is "file"
				Conf{
					"properties": Conf{"type": Conf{"enum": Enum{"file"}}},
				},
				Conf{
					"required": Enum{"file"},
				},
			},
		},
	},
}

type LoggerConfig struct {
	Type    string
	Options *LoggingOptionsConfig
	File    *FileLoggerConfig
}

func GetLoggerConfig(config *viper.Viper) (*LoggerConfig, error) {
	if err := VerifyConfig(config, LoggerSchema); err != nil {
		return nil, err
	}
	c := &LoggerConfig{
		Type: config.GetString("type"),
	}

	if config.InConfig("options") {
		opt, err := GetLoggingOptionsConfig(config.Sub("options"))
		if err != nil {
			return nil, err
		}
		c.Options = opt
	}

	switch c.Type {
	case "file":
		f, err := GetFileLoggerConfig(config.Sub("file"))
		if err != nil {
			return nil, err
		}
		c.File = f
	}
	return c, nil
}

// LoggingSchema is schema for logging configuration
var LoggingSchema = Conf{
	"type":                 "object",
	"additionalProperties": true,
	// "required":             Enum{"type", "listen"},
	"properties": Conf{
		"options": LoggingOptionsSchema,
		"loggers": Conf{
			"type":   "array",
			"iterms": LoggerSchema,
		},
	},
}

type LoggingConfig struct {
	Options *LoggingOptionsConfig
	Loggers []*LoggerConfig
}

func GetLoggingConfig(config *viper.Viper) (*LoggingConfig, error) {
	if err := VerifyConfig(config, LoggingSchema); err != nil {
		return nil, err
	}
	c := &LoggingConfig{}

	if config.InConfig("options") {
		opt, err := GetLoggingOptionsConfig(config.Sub("options"))
		if err != nil {
			return nil, err
		}
		c.Options = opt
	}

	if config.InConfig("loggers") {
		configs, err := GenericArrayToConfigs(config.Get("loggers").([]interface{}))
		if err != nil {
			return nil, err
		}
		for _, cfg := range configs {
			logger, err := GetLoggerConfig(cfg)
			if err != nil {
				return nil, err
			}
			c.Loggers = append(c.Loggers, logger)
		}

	}
	return c, nil
}

// InterfaceSchema is schema for interface configuration
var InterfaceSchema = Conf{
	"type":                 "object",
	"additionalProperties": false,
	"required":             Enum{"type", "listen"},
	"properties": Conf{
		"type":   Conf{"enum": Enum{"rest", "grpc"}},
		"listen": Conf{"type": "string"},
		"secure": Conf{"type": "boolean"},
		"cert":   Conf{"type": "string"},
		"key":    Conf{"type": "string"},
	},
	"allOf": []Conf{
		Conf{
			"anyOf": []Conf{
				// must have "cert" and "key" when "secure" is true
				Conf{
					"properties": Conf{"secure": Conf{"enum": []bool{false}}},
				},
				Conf{
					"required": Enum{"cert", "key"},
				},
			},
		},
	},
}

type InterfaceConfig struct {
	Type   string
	Listen string
	Secure bool
	Cert   string
	Key    string
}

func GetInterfaceConfig(config *viper.Viper) (*InterfaceConfig, error) {
	if err := VerifyConfig(config, InterfaceSchema); err != nil {
		return nil, err
	}
	return &InterfaceConfig{
		Type:   config.GetString("type"),
		Listen: config.GetString("listen"),
		Secure: config.GetBool("secure"),
		Cert:   config.GetString("cert"),
		Key:    config.GetString("key"),
	}, nil
}

// InterfacesSchema is schema for public interfaces configuration
var InterfacesSchema = Conf{
	"type":   "array",
	"iterms": InterfaceSchema,
}

type InterfacesConfig struct {
	Interfaces []*InterfaceConfig
}

// Note this function takes a []interface{} instead of Viper. Viper can not
// handle array properly, so instead of Viper.Sub(), we have to use the result
// of Viper.Get() as input for this case.
func GetInterfacesConfig(config []interface{}) (*InterfacesConfig, error) {
	psc := &InterfacesConfig{}
	configs, err := GenericArrayToConfigs(config)
	if err != nil {
		return nil, err
	}
	for _, cfg := range configs {
		svc, err := GetInterfaceConfig(cfg)
		if err != nil {
			return nil, err
		}
		psc.Interfaces = append(psc.Interfaces, svc)
	}
	return psc, nil
}

// KafkaSchema is schema for Kafka configuration
var KafkaSchema = Conf{
	"type":                 "object",
	"additionalProperties": false,
	"required":             Enum{"brokers", "topic"},
	"properties": Conf{
		"brokers": Conf{"type": "string"},
		"topic":   Conf{"type": "string"},
	},
}

type KafkaConfig struct {
	Brokers string
	Topic   string
}

func GetKafkaConfig(config *viper.Viper) (*KafkaConfig, error) {
	if err := VerifyConfig(config, KafkaSchema); err != nil {
		return nil, err
	}
	return &KafkaConfig{
		Brokers: config.GetString("brokers"),
		Topic:   config.GetString("topic"),
	}, nil
}

// ConsumerSchema is schema for request consumer configuration
var ConsumerSchema = Conf{
	"type":                 "object",
	"additionalProperties": false,
	"required":             Enum{"type"},

	"properties": Conf{
		"batch_size":  Conf{"type": "integer"},
		"max_pending": Conf{"type": "integer"},
		"type":        Conf{"enum": Enum{"kafka"}},
		"kafka":       KafkaSchema,
	},
	"anyOf": []Conf{
		// must have "kafka" when "type" is "kafka"
		Conf{
			"properties": Conf{"type": Conf{"enum": Enum{"kafka"}}},
			"required":   Enum{"kafka"},
		},
	},
}

type ConsumerConfig struct {
	BatchSize  int
	MaxPending int
	Type       string
	Kafka      *KafkaConfig
}

func GetConsumerConfig(config *viper.Viper) (*ConsumerConfig, error) {
	if err := VerifyConfig(config, ConsumerSchema); err != nil {
		return nil, err
	}
	c := &ConsumerConfig{
		BatchSize:  config.GetInt("batch_size"),
		MaxPending: config.GetInt("max_pending"),
		Type:       config.GetString("type"),
	}
	switch c.Type {
	case "kafka":
		kafka, err := GetKafkaConfig(config.Sub("kafka"))
		if err != nil {
			return nil, err
		}
		c.Kafka = kafka
	}
	return c, nil
}

// ProducerSchema is schema for message producer configuration
var ProducerSchema = Conf{
	"type":                 "object",
	"additionalProperties": false,
	"required":             Enum{"type"},

	"properties": Conf{
		"type":  Conf{"enum": Enum{"kafka"}},
		"kafka": KafkaSchema,
	},
	"anyOf": []Conf{
		// must have "kafka" when "type" is "kafka"
		Conf{
			"properties": Conf{"type": Conf{"enum": Enum{"kafka"}}},
			"required":   Enum{"kafka"},
		},
	},
}

type ProducerConfig struct {
	Type  string
	Kafka *KafkaConfig
}

func GetProducerConfig(config *viper.Viper) (*ProducerConfig, error) {
	if err := VerifyConfig(config, ProducerSchema); err != nil {
		return nil, err
	}
	c := &ProducerConfig{
		Type: config.GetString("type"),
	}
	switch c.Type {
	case "kafka":
		kafka, err := GetKafkaConfig(config.Sub("kafka"))
		if err != nil {
			return nil, err
		}
		c.Kafka = kafka
	}
	return c, nil
}

// ElasticSearchSchema is schema for ElasticSearch configuration
var ElasticSearchSchema = Conf{
	"type":                 "object",
	"additionalProperties": false,
	"required":             Enum{"addr"},
	"properties": Conf{
		"addr": Conf{"type": "string"},
	},
}

type ElasticSearchConfig struct {
	Addr string
}

func GetElasticSearchConfig(config *viper.Viper) (*ElasticSearchConfig, error) {
	if err := VerifyConfig(config, ElasticSearchSchema); err != nil {
		return nil, err
	}
	return &ElasticSearchConfig{
		Addr: config.GetString("addr"),
	}, nil
}

// MessageStoreSchema is schema for message store configuration
var MessageStoreSchema = Conf{
	"type":                 "object",
	"additionalProperties": false,

	"properties": Conf{
		"type": Conf{"enum": Enum{"es"}},
		"es":   ElasticSearchSchema,
	},
	"anyOf": []Conf{
		// must have "es" when "type" is "es"
		Conf{
			"properties": Conf{"type": Conf{"enum": Enum{"es"}}},
			"required":   Enum{"es"},
		},
	},
}

type MessageStoreConfig struct {
	Type string
	ES   *ElasticSearchConfig
}

func GetMessageStoreConfig(config *viper.Viper) (*MessageStoreConfig, error) {
	if err := VerifyConfig(config, MessageStoreSchema); err != nil {
		return nil, err
	}
	c := &MessageStoreConfig{
		Type: config.GetString("type"),
	}
	switch c.Type {
	case "es":
		es, err := GetElasticSearchConfig(config.Sub("es"))
		if err != nil {
			return nil, err
		}
		c.ES = es
	}
	return c, nil
}

// BoltSchema is schema for Bolt storage configuration
var BoltSchema = Conf{
	"type":                 "object",
	"additionalProperties": false,
	"required":             Enum{"path"},

	"properties": Conf{
		"path": Conf{"type": "string"},
	},
}

type BoltConfig struct {
	Path string
}

func GetBoltConfig(config *viper.Viper) (*BoltConfig, error) {
	if err := VerifyConfig(config, BoltSchema); err != nil {
		return nil, err
	}
	return &BoltConfig{
		Path: config.GetString("path"),
	}, nil
}

// SessionStoreSchema is schema for session storage configuration
var SessionStoreSchema = Conf{
	"type":                 "object",
	"additionalProperties": false,
	"required":             Enum{"type"},

	"properties": Conf{
		"type": Conf{"enum": Enum{"bolt"}},
		"bolt": BoltSchema,
	},
	"anyOf": []Conf{
		// must have "path" when "type" is "bolt"
		Conf{
			"properties": Conf{"type": Conf{"enum": Enum{"bolt"}}},
			"required":   Enum{"bolt"},
		},
	},
}

type SessionStoreConfig struct {
	Type string
	Bolt *BoltConfig
}

func GetSessionStoreConfig(config *viper.Viper) (*SessionStoreConfig, error) {
	if err := VerifyConfig(config, SessionStoreSchema); err != nil {
		return nil, err
	}
	c := &SessionStoreConfig{
		Type: config.GetString("type"),
	}
	switch c.Type {
	case "bolt":
		bolt, err := GetBoltConfig(config.Sub("bolt"))
		if err != nil {
			return nil, err
		}
		c.Bolt = bolt
	}
	return c, nil
}

// SessionSchema is schema for session configuration
var SessionSchema = Conf{
	"type":                 "object",
	"additionalProperties": false,
	"required":             Enum{"store"},

	"properties": Conf{
		// storage to persist session data
		"store": SessionStoreSchema,
	},
}

type SessionConfig struct {
	Store *SessionStoreConfig
}

func GetSessionConfig(config *viper.Viper) (*SessionConfig, error) {
	if err := VerifyConfig(config, SessionSchema); err != nil {
		return nil, err
	}
	store, err := GetSessionStoreConfig(config.Sub("store"))
	if err != nil {
		return nil, err
	}
	return &SessionConfig{
		Store: store,
	}, nil
}

// VerifyConfig checks if config matches schema.
func VerifyConfig(config *viper.Viper, schema Conf) error {
	var c map[string]interface{}
	if err := config.Unmarshal(&c); err != nil {
		return err
	}
	return VerifyGenericConfig(c, schema)
}

// VerifyGenericConfig checks if config matches schema.
func VerifyGenericConfig(config map[string]interface{}, schema Conf) error {
	schemaLoader := gojsonschema.NewGoLoader(schema)
	configLoader := gojsonschema.NewGoLoader(config)

	result, err := gojsonschema.Validate(schemaLoader, configLoader)
	if err != nil {
		return err
	}

	if result.Valid() {
		return nil
	}
	buf := &bytes.Buffer{}
	for i, desc := range result.Errors() {
		fmt.Fprintf(buf, "(%d) %s ", i, desc)
	}
	return errors.Errorf("Invalid config: %s", buf.String())
}

// GenericArrayToConfigs converts a generic array into an array of viper
// configurations.
func GenericArrayToConfigs(arr []interface{}) ([]*viper.Viper, error) {
	configs := make([]*viper.Viper, len(arr))
	for i, a := range arr {
		data, err := json.Marshal(a)
		if err != nil {
			return nil, err
		}

		c := viper.New()
		c.SetConfigType("json")
		if err := c.ReadConfig(bytes.NewBuffer(data)); err != nil {
			return nil, err
		}
		configs[i] = c
	}
	return configs, nil
}
