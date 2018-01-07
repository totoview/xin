// Code generated -- DO NOT EDIT.
package store

{{range $index, $type := .}}
func NewEventWith{{ $type }}(event *{{ $type }}) StoreEvent {
	return StoreEvent{ Type: StoreEvent_{{ $type }}, {{ $type }}: event }
}
{{end}}
