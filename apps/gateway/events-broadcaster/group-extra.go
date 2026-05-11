package events_broadcaster

import "github.com/walkline/ToCloud9/shared/events"

const (
	EventTypeGroupReadyCheckStarted EventType = 10000 + iota
	EventTypeGroupReadyCheckMemberState
	EventTypeGroupReadyCheckFinished
	EventTypeGroupMemberSubGroupChanged
	EventTypeGroupMemberFlagsChanged
	EventTypeGroupMemberStateChanged
)

func (b *broadcasterImpl) NewGroupReadyCheckStartedEvent(payload *events.GroupEventReadyCheckStartedPayload) {
	for _, ch := range b.channelsForGUIDs(payload.Receivers) {
		ch <- Event{Type: EventTypeGroupReadyCheckStarted, Payload: payload}
	}
}

func (b *broadcasterImpl) NewGroupReadyCheckMemberStateEvent(payload *events.GroupEventReadyCheckMemberStatePayload) {
	for _, ch := range b.channelsForGUIDs(payload.Receivers) {
		ch <- Event{Type: EventTypeGroupReadyCheckMemberState, Payload: payload}
	}
}

func (b *broadcasterImpl) NewGroupReadyCheckFinishedEvent(payload *events.GroupEventReadyCheckFinishedPayload) {
	for _, ch := range b.channelsForGUIDs(payload.Receivers) {
		ch <- Event{Type: EventTypeGroupReadyCheckFinished, Payload: payload}
	}
}

func (b *broadcasterImpl) NewGroupMemberSubGroupChangedEvent(payload *events.GroupEventMemberSubGroupChangedPayload) {
	for _, ch := range b.channelsForGUIDs(payload.Receivers) {
		ch <- Event{Type: EventTypeGroupMemberSubGroupChanged, Payload: payload}
	}
}

func (b *broadcasterImpl) NewGroupMemberFlagsChangedEvent(payload *events.GroupEventMemberFlagsChangedPayload) {
	for _, ch := range b.channelsForGUIDs(payload.Receivers) {
		ch <- Event{Type: EventTypeGroupMemberFlagsChanged, Payload: payload}
	}
}

func (b *broadcasterImpl) NewGroupMemberStateChangedEvent(payload *events.GroupEventMemberStateChangedPayload) {
	for _, ch := range b.channelsForGUIDs(payload.Receivers) {
		ch <- Event{Type: EventTypeGroupMemberStateChanged, Payload: payload}
	}
}
