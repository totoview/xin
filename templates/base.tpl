// Code generated -- DO NOT EDIT.
package pb

{{range $index, $type := .ChannelTypes}}
func NewChannelWith{{ $type }}(channel *{{ $type }}Channel) MsgChannel {
	return MsgChannel{ Type: MsgChannel_{{ $type }}, {{ $type }}: channel }
}
{{end}}

{{range $index, $type := .UserTypes}}
func NewUserWith{{ $type }}(user *{{ $type }}User) MsgUser {
	return MsgUser{ Type: MsgUser_{{ $type }}, {{ $type }}: user }
}
{{end}}