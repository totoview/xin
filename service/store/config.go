package store

import (
	"github.com/totoview/xin/core"
)

// Schema is store service's configuration schema.
var Schema = core.Conf{
	"$schema":              "http://json-schema.org/draft-06/schema#",
	"type":                 "object",
	"additionalProperties": false,
	"required":             core.Enum{"session", "source", "store", "update"},

	"properties": core.Conf{

		// session management
		"session": core.SessionSchema,

		// source of incoming messages
		"source": core.ConsumerSchema,

		// actual message storage
		"store": core.MessageStoreSchema,

		// update event stream for down-stream services that rely on
		// message store update events
		"update": core.ProducerSchema,
	},
}
