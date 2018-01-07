// Code generated -- DO NOT EDIT.
package mgw

import "time"


func NewRequestWithMsgReq(msg *MsgReq) MgwRequest {
	return MgwRequest{ Type: MgwRequest_MsgReq, MsgReq: msg, TsNanos: uint64(time.Now().UnixNano()) }
}



func NewEventWithMsgDelivery(event *MsgDelivery) MgwEvent {
	return MgwEvent{ Type: MgwEvent_MsgDelivery, MsgDelivery: event }
}
