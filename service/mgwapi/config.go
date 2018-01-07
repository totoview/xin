package mgwapi

import (
	"github.com/totoview/xin/core"
)

// Schema is store service's configuration schema.
var Schema = core.Conf{
	"$schema":              "http://json-schema.org/draft-06/schema#",
	"type":                 "object",
	"additionalProperties": false,
	"required":             core.Enum{"session", "interfaces", "update"},

	"properties": core.Conf{

		// public interfaces
		"interfaces": core.InterfacesSchema,

		// session management
		"session": core.SessionSchema,

		// update for mgw
		"update": core.ProducerSchema,
	},
}
