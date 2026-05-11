package session

import (
	"context"
	"fmt"

	root "github.com/walkline/ToCloud9/apps/gateway"
	"github.com/walkline/ToCloud9/apps/gateway/packet"
	pbChar "github.com/walkline/ToCloud9/gen/characters/pb"
	"github.com/walkline/ToCloud9/gen/group/pb"
)

const (
	readyCheckDefaultDurationMs uint32 = 35000

	groupMemberFlagAssistant  uint8 = 0x01
	groupMemberFlagMainTank   uint8 = 0x02
	groupMemberFlagMainAssist uint8 = 0x04
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

	state := r.Uint8()
	if err := r.Error(); err != nil {
		return err
	}

	_, err := s.groupServiceClient.SetReadyCheckMemberState(ctx, &pb.SetReadyCheckMemberStateRequest{
		Api:        root.SupportedGroupServiceVer,
		RealmID:    root.RealmID,
		MemberGUID: s.character.GUID,
		State:      uint32(state),
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