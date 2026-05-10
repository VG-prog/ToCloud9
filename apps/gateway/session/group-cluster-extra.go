package session

import (
	"context"
	"fmt"

	root "github.com/walkline/ToCloud9/apps/gateway"
	eBroadcaster "github.com/walkline/ToCloud9/apps/gateway/events-broadcaster"
	"github.com/walkline/ToCloud9/apps/gateway/packet"
	pbChar "github.com/walkline/ToCloud9/gen/characters/pb"
	"github.com/walkline/ToCloud9/gen/group/pb"
	"github.com/walkline/ToCloud9/shared/events"
)

const (
	readyCheckDefaultDurationMs uint32 = 35000

	groupMemberFlagAssistant  uint8 = 0x01
	groupMemberFlagMainTank   uint8 = 0x02
	groupMemberFlagMainAssist uint8 = 0x04

	memberStatusOffline uint16 = 0x0000
	memberStatusOnline  uint16 = 0x0001

	groupUpdateFlagStatus    uint32 = 0x00000001
	groupUpdateFlagCurHP     uint32 = 0x00000002
	groupUpdateFlagMaxHP     uint32 = 0x00000004
	groupUpdateFlagPowerType uint32 = 0x00000008
	groupUpdateFlagCurPower  uint32 = 0x00000010
	groupUpdateFlagMaxPower  uint32 = 0x00000020
	groupUpdateFlagLevel     uint32 = 0x00000040
	groupUpdateFlagZone      uint32 = 0x00000080
)

func (s *GameSession) HandleRaidReadyCheck(ctx context.Context, p *packet.Packet) error {
	if p.Source == packet.SourceWorldServer {
		s.gameSocket.WriteChannel() <- p
		return nil
	}

	r := p.Reader()

	if r.Left() == 0 {
		_, err := s.groupServiceClient.StartReadyCheck(ctx, &pb.StartReadyCheckRequest{
			Api:        root.SupportedGroupServiceVer,
			RealmID:    root.RealmID,
			LeaderGUID: s.character.GUID,
			DurationMs: readyCheckDefaultDurationMs,
		})
		return err
	}

	clientState := r.Uint8()
	if err := r.Error(); err != nil {
		return err
	}

	state := uint32(2) // not ready
	if clientState != 0 {
		state = 1 // ready
	}

	_, err := s.groupServiceClient.SetReadyCheckMemberState(ctx, &pb.SetReadyCheckMemberStateRequest{
		Api:        root.SupportedGroupServiceVer,
		RealmID:    root.RealmID,
		MemberGUID: s.character.GUID,
		State:      state,
	})
	return err
}

func (s *GameSession) HandleGroupChangeSubGroup(ctx context.Context, p *packet.Packet) error {
	if p.Source == packet.SourceWorldServer {
		s.gameSocket.WriteChannel() <- p
		return nil
	}

	r := p.Reader()
	memberName := r.String()
	subGroup := r.Uint8()

	if err := r.Error(); err != nil {
		return err
	}

	charResp, err := s.charServiceClient.CharacterByName(ctx, &pbChar.CharacterByNameRequest{
		Api:           root.SupportedCharServiceVer,
		RealmID:       root.RealmID,
		CharacterName: memberName,
	})
	if err != nil {
		return err
	}

	if charResp.Character == nil {
		return fmt.Errorf("group member %q not found", memberName)
	}

	_, err = s.groupServiceClient.ChangeMemberSubGroup(ctx, &pb.ChangeMemberSubGroupRequest{
		Api:         root.SupportedGroupServiceVer,
		RealmID:     root.RealmID,
		UpdaterGUID: s.character.GUID,
		MemberGUID:  charResp.Character.CharGUID,
		SubGroup:    uint32(subGroup),
	})
	return err
}

func (s *GameSession) HandleGroupAssistantLeader(ctx context.Context, p *packet.Packet) error {
	if p.Source == packet.SourceWorldServer {
		s.gameSocket.WriteChannel() <- p
		return nil
	}

	r := p.Reader()

	memberGUID := readGUIDThenBoolCompatible(r)
	apply := readLastBoolCompatible(r)

	if err := r.Error(); err != nil {
		return err
	}

	return s.setGroupMemberFlag(ctx, memberGUID, groupMemberFlagAssistant, apply)
}

func (s *GameSession) HandlePartyAssignment(ctx context.Context, p *packet.Packet) error {
	if p.Source == packet.SourceWorldServer {
		s.gameSocket.WriteChannel() <- p
		return nil
	}

	r := p.Reader()

	assignment := r.Uint8()
	apply := r.Uint8() != 0
	memberGUID := readRemainingGUIDCompatible(r)

	if err := r.Error(); err != nil {
		return err
	}

	var flag uint8
	switch assignment {
	case 0:
		flag = groupMemberFlagMainTank
	case 1:
		flag = groupMemberFlagMainAssist
	default:
		return nil
	}

	return s.setGroupMemberFlag(ctx, memberGUID, flag, apply)
}

func (s *GameSession) HandleResetInstances(ctx context.Context, p *packet.Packet) error {
	if p.Source == packet.SourceWorldServer {
		s.gameSocket.WriteChannel() <- p
		return nil
	}

	_, err := s.groupServiceClient.ResetInstance(ctx, &pb.ResetInstanceRequest{
		Api:        root.SupportedGroupServiceVer,
		RealmID:    root.RealmID,
		PlayerGUID: s.character.GUID,
		MapID:      0,
		Difficulty: 0,
	})
	return err
}

func (s *GameSession) HandleSetSavedInstanceExtend(ctx context.Context, p *packet.Packet) error {
	if p.Source == packet.SourceWorldServer {
		s.gameSocket.WriteChannel() <- p
		return nil
	}

	r := p.Reader()

	mapID := r.Uint32()
	difficulty := r.Uint32()
	extended := r.Uint8() != 0

	if err := r.Error(); err != nil {
		return err
	}

	_, err := s.groupServiceClient.SetInstanceBindExtension(ctx, &pb.SetInstanceBindExtensionRequest{
		Api:        root.SupportedGroupServiceVer,
		RealmID:    root.RealmID,
		PlayerGUID: s.character.GUID,
		MapID:      mapID,
		Difficulty: difficulty,
		Extended:   extended,
	})
	return err
}

func (s *GameSession) setGroupMemberFlag(ctx context.Context, memberGUID uint64, flag uint8, apply bool) error {
	groupResp, err := s.groupServiceClient.GetGroupByMember(ctx, &pb.GetGroupByMemberRequest{
		Api:     root.SupportedGroupServiceVer,
		RealmID: root.RealmID,
		Player:  s.character.GUID,
	})
	if err != nil {
		return err
	}

	if groupResp.Group == nil {
		return nil
	}

	var current *pb.GetGroupResponse_GroupMember
	for _, member := range groupResp.Group.Members {
		if member.Guid == memberGUID {
			current = member
			break
		}
	}

	if current == nil {
		return nil
	}

	flags := uint8(current.Flags)
	if apply {
		flags |= flag
	} else {
		flags &^= flag
	}

	_, err = s.groupServiceClient.SetMemberFlags(ctx, &pb.SetMemberFlagsRequest{
		Api:         root.SupportedGroupServiceVer,
		RealmID:     root.RealmID,
		UpdaterGUID: s.character.GUID,
		MemberGUID:  memberGUID,
		Flags:       uint32(flags),
		Roles:       current.Roles,
	})
	return err
}

func (s *GameSession) HandleEventGroupReadyCheckStarted(ctx context.Context, event *eBroadcaster.Event) error {
	payload := event.Payload.(*events.GroupEventReadyCheckStartedPayload)

	w := packet.NewWriterWithSize(packet.MsgRaidReadyCheck, 8)
	w.Uint64(payload.LeaderGUID)

	s.gameSocket.Send(w)
	return nil
}

func (s *GameSession) HandleEventGroupReadyCheckMemberState(ctx context.Context, event *eBroadcaster.Event) error {
	payload := event.Payload.(*events.GroupEventReadyCheckMemberStatePayload)

	if payload.State == 0 {
		return nil
	}

	confirm := uint8(0)
	if payload.State == 1 {
		confirm = 1
	}

	w := packet.NewWriterWithSize(packet.MsgRaidReadyCheckConfirm, 9)
	w.Uint64(payload.MemberGUID)
	w.Uint8(confirm)

	s.gameSocket.Send(w)
	return nil
}

func (s *GameSession) HandleEventGroupReadyCheckFinished(ctx context.Context, event *eBroadcaster.Event) error {
	w := packet.NewWriterWithSize(packet.MsgRaidReadyCheckFinished, 0)
	s.gameSocket.Send(w)

	return nil
}

func (s *GameSession) HandleEventGroupMemberSubGroupChanged(ctx context.Context, event *eBroadcaster.Event) error {
	payload := event.Payload.(*events.GroupEventMemberSubGroupChangedPayload)
	return s.SendGroupUpdate(ctx, payload.GroupID)
}

func (s *GameSession) HandleEventGroupMemberFlagsChanged(ctx context.Context, event *eBroadcaster.Event) error {
	payload := event.Payload.(*events.GroupEventMemberFlagsChangedPayload)
	return s.SendGroupUpdate(ctx, payload.GroupID)
}

func (s *GameSession) HandleEventGroupMemberStateChanged(ctx context.Context, event *eBroadcaster.Event) error {
	payload := event.Payload.(*events.GroupEventMemberStateChangedPayload)

	if payload.MemberGUID == s.character.GUID {
		return nil
	}

	s.sendPartyMemberStats(payload)
	return nil
}

const (
	powerMana       uint8 = 0
	powerRage       uint8 = 1
	powerEnergy     uint8 = 3
	powerRunicPower uint8 = 6

	classWarrior     uint8 = 1
	classRogue       uint8 = 4
	classDeathKnight uint8 = 6
)

func defaultPowerTypeForClass(class uint8) uint8 {
	switch class {
	case classWarrior:
		return powerRage
	case classRogue:
		return powerEnergy
	case classDeathKnight:
		return powerRunicPower
	default:
		return powerMana
	}
}

func (s *GameSession) sendPartyMemberStats(payload *events.GroupEventMemberStateChangedPayload) {
	status := memberStatusOffline
	if payload.Online {
		status = memberStatusOnline
	}

	mask := groupUpdateFlagStatus |
		groupUpdateFlagCurHP |
		groupUpdateFlagMaxHP |
		groupUpdateFlagPowerType |
		groupUpdateFlagCurPower |
		groupUpdateFlagMaxPower |
		groupUpdateFlagLevel |
		groupUpdateFlagZone

	health := clampPct16(payload.HealthPct)
	power := clampPct16(payload.PowerPct)

	w := packet.NewWriterWithSize(packet.SMsgPartyMemberStatsFull, 64)
	w.GUID(payload.MemberGUID) // debe escribir packed GUID
	w.Uint32(mask)
	w.Uint16(status)
	w.Uint32(uint32(health))
	w.Uint32(100)
	w.Uint8(defaultPowerTypeForClass(payload.Class))
	w.Uint16(power)
	w.Uint16(100)
	w.Uint16(uint16(payload.Level))
	w.Uint16(uint16(payload.ZoneID))

	s.gameSocket.Send(w)
}

func readGUIDThenBoolCompatible(r *packet.Reader) uint64 {
	if r.Left() == 9 {
		return r.Uint64()
	}

	return r.ReadGUID()
}

func readLastBoolCompatible(r *packet.Reader) bool {
	if r.Left() == 0 {
		return false
	}

	return r.Uint8() != 0
}

func readRemainingGUIDCompatible(r *packet.Reader) uint64 {
	if r.Left() == 8 {
		return r.Uint64()
	}

	return r.ReadGUID()
}

func clampPct16(v uint16) uint16 {
	if v > 100 {
		return 100
	}

	return v
}
