// Code generated -- DO NOT EDIT.
package store

func NewEventWithMsgCreated(event *MsgCreated) StoreEvent {
	return StoreEvent{Type: StoreEvent_MsgCreated, MsgCreated: event}
}

func NewEventWithMsgUpdated(event *MsgUpdated) StoreEvent {
	return StoreEvent{Type: StoreEvent_MsgUpdated, MsgUpdated: event}
}

func NewEventWithMsgDeleted(event *MsgDeleted) StoreEvent {
	return StoreEvent{Type: StoreEvent_MsgDeleted, MsgDeleted: event}
}
