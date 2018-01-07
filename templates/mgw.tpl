// Code generated -- DO NOT EDIT.
package mgw

import "time"

{{range $index, $type := .RequestTypes}}
func NewRequestWith{{ $type }}(msg *{{ $type }}) MgwRequest {
	return MgwRequest{ Type: MgwRequest_{{ $type }}, {{ $type }}: msg, TsNanos: uint64(time.Now().UnixNano()) }
}
{{end}}

{{range $index, $type := .EventTypes}}
func NewEventWith{{ $type }}(event *{{ $type }}) MgwEvent {
	return MgwEvent{ Type: MgwEvent_{{ $type }}, {{ $type }}: event }
}
{{end}}