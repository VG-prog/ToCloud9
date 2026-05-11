package service

import (
	"github.com/nats-io/nats.go"
	eBroadcaster "github.com/walkline/ToCloud9/apps/gateway/events-broadcaster"
	"github.com/walkline/ToCloud9/shared/events"
)

func subscribeGroupPayload[T any](l *groupNatsListener, event events.GroupServiceEvent, cb func(*T)) error {
	sub, err := l.nc.Subscribe(event.SubjectName(), func(msg *nats.Msg) {
		payload := new(T)

		_, err := events.Unmarshal(msg.Data, payload)
		if err != nil {
			return
		}

		cb(payload)
	})
	if err != nil {
		return err
	}

	l.extraSubs = append(l.extraSubs, sub)
	return nil
}

func (l *groupNatsListener) listenExtraGroupEvents() error {
	if b, ok := l.broadcaster.(interface {
		NewGroupReadyCheckStartedEvent(*events.GroupEventReadyCheckStartedPayload)
	}); ok {
		if err := subscribeGroupPayload(l, events.GroupEventGroupReadyCheckStarted, b.NewGroupReadyCheckStartedEvent); err != nil {
			return err
		}
	}

	if b, ok := l.broadcaster.(interface {
		NewGroupReadyCheckMemberStateEvent(*events.GroupEventReadyCheckMemberStatePayload)
	}); ok {
		if err := subscribeGroupPayload(l, events.GroupEventGroupReadyCheckMemberState, b.NewGroupReadyCheckMemberStateEvent); err != nil {
			return err
		}
	}

	if b, ok := l.broadcaster.(interface {
		NewGroupReadyCheckFinishedEvent(*events.GroupEventReadyCheckFinishedPayload)
	}); ok {
		if err := subscribeGroupPayload(l, events.GroupEventGroupReadyCheckFinished, b.NewGroupReadyCheckFinishedEvent); err != nil {
			return err
		}
	}

	if b, ok := l.broadcaster.(interface {
		NewGroupMemberSubGroupChangedEvent(*events.GroupEventMemberSubGroupChangedPayload)
	}); ok {
		if err := subscribeGroupPayload(l, events.GroupEventGroupMemberSubGroupChanged, b.NewGroupMemberSubGroupChangedEvent); err != nil {
			return err
		}
	}

	if b, ok := l.broadcaster.(interface {
		NewGroupMemberFlagsChangedEvent(*events.GroupEventMemberFlagsChangedPayload)
	}); ok {
		if err := subscribeGroupPayload(l, events.GroupEventGroupMemberFlagsChanged, b.NewGroupMemberFlagsChangedEvent); err != nil {
			return err
		}
	}

	if b, ok := l.broadcaster.(interface {
		NewGroupMemberStateChangedEvent(*events.GroupEventMemberStateChangedPayload)
	}); ok {
		if err := subscribeGroupPayload(l, events.GroupEventGroupMemberStateChanged, b.NewGroupMemberStateChangedEvent); err != nil {
			return err
		}
	}

	_ = eBroadcaster.EventTypeGroupReadyCheckStarted
	return nil
}
