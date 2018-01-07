package query

import (
	"github.com/totoview/xin/core"
)

// Schema is store service's configuration schema.
var Schema = core.Conf{
	"$schema":              "http://json-schema.org/draft-06/schema#",
	"type":                 "object",
	"additionalProperties": false,
	"required":             core.Enum{"session", "source"},

	"properties": core.Conf{

		// session management
		"session": core.SessionSchema,

		// source of incoming store updates
		"source": core.ConsumerSchema,
	},
}
