package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/walkline/ToCloud9/apps/groupserver"
	"github.com/walkline/ToCloud9/apps/groupserver/repo"
	"github.com/walkline/ToCloud9/gen/characters/pb"
	"github.com/walkline/ToCloud9/shared/events"
)

var (
	ErrAlreadyInGroup        = errors.New("player already in group")
	ErrNoPermissions         = errors.New("player has not enough permissions")
	ErrGroupFull             = errors.New("group is full")
	ErrGroupNotFound         = errors.New("group not found")
	ErrGroupMemberNotFound   = errors.New("group member not found")
	ErrMemberInDungeonOrRaid = errors.New("group member is in dungeon or raid")
	ErrInviteNotFound        = errors.New("invite not found")
)

type MessageType uint8

const (
	MessageTypeGroup       MessageType = 0x2
	MessageTypeGroupLeader MessageType = 0x33
	MessageTypeRaid        MessageType = 0x3
	MessageTypeRaidLeader  MessageType = 0x27
)

type GroupsService interface {
	GroupByID(ctx context.Context, realmID uint32, groupID uint) (*repo.Group, error)
	GroupByMemberGUID(ctx context.Context, realmID uint32, memberGUID uint64) (*repo.Group, error)
	GroupIDByPlayer(ctx context.Context, realmID uint32, player uint64) (uint, error)

	Invite(ctx context.Context, realmID uint32, inviter, invited uint64, inviterName, invitedName string) error
	Uninvite(ctx context.Context, realmID uint32, initiator, target uint64, reason string) error
	Leave(ctx context.Context, realmID uint32, player uint64) error

	ChangeLeader(ctx context.Context, realmID uint32, player, newLeader uint64) error
	ConvertToRaid(ctx context.Context, realmID uint32, player uint64) error

	AcceptInvite(ctx context.Context, realmID uint32, player uint64) error

	SendMessage(ctx context.Context, realmID uint32, senderGUID uint64, message string, lang uint32, messageType MessageType) error

	SetTargetIcon(ctx context.Context, realmID uint32, updaterGUID uint64, iconID uint8, targetGUID uint64) error
	SetLootMethod(ctx context.Context, realmID uint32, updaterGUID uint64, method uint8, lootMaster uint64, lootThreshold uint8) error

	SetDungeonDifficulty(ctx context.Context, realmID uint32, updaterGUID uint64, difficulty uint8) error
	SetRaidDifficulty(ctx context.Context, realmID uint32, updaterGUID uint64, difficulty uint8) error

	StartReadyCheck(ctx context.Context, realmID uint32, leaderGUID uint64, durationMs uint32) error
	SetReadyCheckMemberState(ctx context.Context, realmID uint32, memberGUID uint64, state uint8) error
	FinishReadyCheck(ctx context.Context, realmID uint32, playerGUID uint64) error
	ChangeMemberSubGroup(ctx context.Context, realmID uint32, updaterGUID, memberGUID uint64, subGroup uint8) error
	SetMemberFlags(ctx context.Context, realmID uint32, updaterGUID, memberGUID uint64, flags, roles uint8) error
	UpdateMemberState(ctx context.Context, realmID uint32, memberGUID uint64, online bool, level, class uint8, zoneID, mapID uint32, healthPct, powerPct uint16) error
	ResetInstance(ctx context.Context, realmID uint32, playerGUID uint64, mapID uint32, difficulty uint8) error
	SetInstanceBindExtension(ctx context.Context, realmID uint32, playerGUID uint64, mapID uint32, difficulty uint8, extended bool) error

	// GWCharacterLoggedInHandler updates cache with player logged in.
	events.GWCharacterLoggedInHandler
	// GWCharacterLoggedOutHandler updates cache with player logged out.
	events.GWCharacterLoggedOutHandler
}

func subgroupMemberCount(group *repo.Group, subGroup uint8, exceptGUID uint64) int {
	count := 0

	for _, member := range group.Members {
		if member.MemberGUID == exceptGUID {
			continue
		}

		if member.SubGroup == subGroup {
			count++
		}
	}

	return count
}

func NewGroupsService(r repo.GroupsRepo, charClient pb.CharactersServiceClient, ep events.GroupServiceProducer) GroupsService {
	return &groupServiceImpl{
		r:          r,
		ep:         ep,
		charClient: charClient,
	}
}

type groupServiceImpl struct {
	r  repo.GroupsRepo
	ep events.GroupServiceProducer

	charClient pb.CharactersServiceClient
}

func (g groupServiceImpl) GroupIDByPlayer(ctx context.Context, realmID uint32, player uint64) (uint, error) {
	return g.r.GroupIDByPlayer(ctx, realmID, player)
}

func (g groupServiceImpl) GroupByID(ctx context.Context, realmID uint32, groupID uint) (*repo.Group, error) {
	return g.r.GroupByID(ctx, realmID, groupID, true)
}

func (g groupServiceImpl) GroupByMemberGUID(ctx context.Context, realmID uint32, memberGUID uint64) (*repo.Group, error) {
	groupID, err := g.GroupIDByPlayer(ctx, realmID, memberGUID)
	if err != nil {
		return nil, err
	}

	return g.GroupByID(ctx, realmID, groupID)
}

func (g groupServiceImpl) Invite(ctx context.Context, realmID uint32, inviter, invited uint64, inviterName, invitedName string) error {
	groupID, err := g.r.GroupIDByPlayer(ctx, realmID, invited)
	if err != nil {
		return err
	}

	if groupID != 0 {
		return ErrAlreadyInGroup
	}

	inviterGroupID, err := g.r.GroupIDByPlayer(ctx, realmID, inviter)
	if err != nil {
		return err
	}

	if inviterGroupID == 0 {
		if err = g.r.AddInvite(ctx, realmID, repo.GroupInvite{
			Inviter:     inviter,
			InviterName: inviterName,
			Invitee:     invited,
			InviteeName: invitedName,
			GroupID:     0,
		}); err != nil {
			return err
		}

		err = g.ep.InviteCreated(&events.GroupEventInviteCreatedPayload{
			ServiceID:   groupserver.ServiceID,
			RealmID:     realmID,
			GroupID:     0,
			InviterGUID: inviter,
			InviterName: inviterName,
			InviteeGUID: invited,
			InviteeName: invitedName,
		})

		if err != nil {
			log.Error().Err(err).Msg("can't create invite created event")
		}

		return nil
	}

	group, err := g.r.GroupByID(ctx, realmID, inviterGroupID, true)
	if err != nil {
		return err
	}

	member := group.MemberByGUID(inviter)
	if member == nil {
		return fmt.Errorf("can't find player %d in the guild %d", inviter, inviterGroupID)
	}

	if !(group.LeaderGUID == inviter || member.IsAssistant()) {
		return ErrNoPermissions
	}

	if group.IsFull() {
		return ErrGroupFull
	}

	if err = g.r.AddInvite(ctx, realmID, repo.GroupInvite{
		Inviter:     inviter,
		InviterName: inviterName,
		Invitee:     invited,
		InviteeName: invitedName,
		GroupID:     inviterGroupID,
	}); err != nil {
		return err
	}

	err = g.ep.InviteCreated(&events.GroupEventInviteCreatedPayload{
		ServiceID:   groupserver.ServiceID,
		RealmID:     realmID,
		GroupID:     inviterGroupID,
		InviterGUID: inviter,
		InviterName: inviterName,
		InviteeGUID: invited,
		InviteeName: invitedName,
	})

	if err != nil {
		log.Error().Err(err).Msg("can't create invite created event")
	}

	return nil
}

func (g groupServiceImpl) AcceptInvite(ctx context.Context, realmID uint32, player uint64) error {
	invite, err := g.r.GetInviteByInvitedPlayer(ctx, realmID, player)
	if err != nil {
		return err
	}

	if invite == nil {
		return ErrInviteNotFound
	}

	if invite.GroupID == 0 {
		return g.createGroup(ctx, realmID, invite)
	}

	group, err := g.r.GroupByID(ctx, realmID, invite.GroupID, true)
	if err != nil {
		return err
	}

	return g.addMember(ctx, realmID, group, invite)
}

func (g groupServiceImpl) Uninvite(ctx context.Context, realmID uint32, initiator, target uint64, reason string) error {
	groupID, err := g.r.GroupIDByPlayer(ctx, realmID, initiator)
	if err != nil {
		return fmt.Errorf("can't get groupID, err: %w", err)
	}
	if groupID == 0 {
		return ErrGroupNotFound
	}

	group, err := g.r.GroupByID(ctx, realmID, groupID, true)
	if err != nil {
		return fmt.Errorf("can't get group, err: %w", err)
	}

	if group == nil {
		return ErrGroupNotFound
	}

	targetMember := group.MemberByGUID(target)
	if targetMember == nil {
		return ErrGroupNotFound
	}

	if group.LeaderGUID != initiator {
		return ErrNoPermissions
	}

	membersCount := len(group.Members)

	if membersCount <= 2 {
		if err = g.disband(ctx, realmID, group); err != nil {
			return fmt.Errorf("can't disband group, err: %w", err)
		}
	} else {
		eventToSend := events.GroupEventGroupMemberLeftPayload{
			ServiceID:     groupserver.ServiceID,
			RealmID:       realmID,
			GroupID:       groupID,
			MemberGUID:    targetMember.MemberGUID,
			MemberName:    targetMember.MemberName,
			NewLeaderID:   group.LeaderGUID,
			OnlineMembers: group.OnlineMemberGUIDs(),
		}
		if err = g.r.RemoveMember(ctx, realmID, target); err != nil {
			return fmt.Errorf("can't remove member, err: %w", err)
		}

		err = g.ep.GroupMemberLeft(&eventToSend)
		if err != nil {
			log.Error().Err(err).Msg("can't create GroupMemberLeft event")
		}
	}

	return nil
}

func (g groupServiceImpl) Leave(ctx context.Context, realmID uint32, player uint64) error {
	groupID, err := g.r.GroupIDByPlayer(ctx, realmID, player)
	if err != nil {
		return fmt.Errorf("can't get groupID, err: %w", err)
	}
	if groupID == 0 {
		return ErrGroupNotFound
	}

	group, err := g.r.GroupByID(ctx, realmID, groupID, true)
	if err != nil {
		return fmt.Errorf("can't get group, err: %w", err)
	}

	member := group.MemberByGUID(player)
	if member == nil {
		return ErrGroupNotFound
	}

	if len(group.Members) <= 2 {
		return g.disband(ctx, realmID, group)
	}

	if player == group.LeaderGUID {
		var newLeader uint64
		for _, groupMember := range group.Members {
			if !groupMember.IsOnline || groupMember.MemberGUID == player {
				continue
			}

			newLeader = groupMember.MemberGUID
			break
		}

		if err = g.changeLeader(ctx, realmID, group, newLeader, false); err != nil {
			return fmt.Errorf("can't change group leader, err: %w", err)
		}
	}

	eventToSend := events.GroupEventGroupMemberLeftPayload{
		ServiceID:     groupserver.ServiceID,
		RealmID:       realmID,
		GroupID:       groupID,
		MemberGUID:    member.MemberGUID,
		MemberName:    member.MemberName,
		NewLeaderID:   group.LeaderGUID,
		OnlineMembers: group.OnlineMemberGUIDs(),
	}

	if err = g.r.RemoveMember(ctx, realmID, player); err != nil {
		return fmt.Errorf("can't remove group member, err: %w", err)
	}

	err = g.ep.GroupMemberLeft(&eventToSend)
	if err != nil {
		log.Error().Err(err).Msg("can't create GroupMemberLeft event")
	}

	return nil
}

func (g groupServiceImpl) ChangeLeader(ctx context.Context, realmID uint32, player, newLeader uint64) error {
	group, err := g.getGroupWithLeader(ctx, realmID, player)
	if err != nil {
		return err
	}

	newLeaderMember := group.MemberByGUID(newLeader)
	if newLeaderMember == nil {
		return ErrGroupNotFound
	}

	return g.changeLeader(ctx, realmID, group, newLeader, true)
}

func (g groupServiceImpl) ConvertToRaid(ctx context.Context, realmID uint32, player uint64) error {
	group, err := g.getGroupWithLeader(ctx, realmID, player)
	if err != nil {
		return err
	}

	group.GroupType |= repo.GroupTypeFlagsRaid
	if err := g.r.Update(ctx, realmID, group); err != nil {
		return fmt.Errorf("can't update group win a new leader, err: %w", err)
	}
	err = g.ep.ConvertedToRaid(&events.GroupEventGroupConvertedToRaidPayload{
		ServiceID:     groupserver.ServiceID,
		RealmID:       realmID,
		GroupID:       group.ID,
		Leader:        group.LeaderGUID,
		OnlineMembers: group.OnlineMemberGUIDs(),
	})
	if err != nil {
		log.Error().Err(err).Msg("can't create ConvertedToRaid event")
	}

	return nil
}

func (g groupServiceImpl) SendMessage(ctx context.Context, realmID uint32, senderGUID uint64, message string, lang uint32, messageType MessageType) error {
	groupID, err := g.r.GroupIDByPlayer(ctx, realmID, senderGUID)
	if err != nil {
		return fmt.Errorf("can't get groupID, err: %w", err)
	}
	if groupID == 0 {
		return ErrGroupNotFound
	}

	group, err := g.r.GroupByID(ctx, realmID, groupID, true)
	if err != nil {
		return fmt.Errorf("can't get group, err: %w", err)
	}

	if group == nil {
		return ErrGroupNotFound
	}

	member := group.MemberByGUID(senderGUID)
	if member == nil {
		return ErrGroupMemberNotFound
	}

	isLeader := false
	switch messageType {
	case MessageTypeGroup, MessageTypeRaid:
		isLeader = false
	case MessageTypeGroupLeader, MessageTypeRaidLeader:
		isLeader = true
	default:
		return fmt.Errorf("message with type %d unsupported", messageType)
	}

	if isLeader && group.LeaderGUID != senderGUID {
		return ErrNoPermissions
	}

	err = g.ep.SendChatMessage(&events.GroupEventNewMessagePayload{
		ServiceID:   groupserver.ServiceID,
		RealmID:     realmID,
		GroupID:     group.ID,
		SenderGUID:  senderGUID,
		SenderName:  member.MemberName,
		Language:    lang,
		Msg:         message,
		MessageType: uint8(messageType),
		Receivers:   group.OnlineMemberGUIDs(),
	})
	if err != nil {
		log.Error().Err(err).Msg("can't create SendChatMessage event")
	}

	return nil
}

func (g groupServiceImpl) SetTargetIcon(ctx context.Context, realmID uint32, updaterGUID uint64, iconID uint8, targetGUID uint64) error {
	if repo.MaxTargetIcons <= iconID {
		return fmt.Errorf("iconID (%d) is invalid", iconID)
	}

	groupID, err := g.r.GroupIDByPlayer(ctx, realmID, updaterGUID)
	if err != nil {
		return fmt.Errorf("can't get groupID, err: %w", err)
	}
	if groupID == 0 {
		return ErrGroupNotFound
	}

	group, err := g.r.GroupByID(ctx, realmID, groupID, true)
	if err != nil {
		return fmt.Errorf("can't get group, err: %w", err)
	}

	if group == nil {
		return ErrGroupNotFound
	}

	groupMember := group.MemberByGUID(updaterGUID)
	if group.IsRaid() && group.LeaderGUID != updaterGUID && !groupMember.IsAssistant() {
		return ErrNoPermissions
	}

	for i, target := range group.TargetIcons {
		if target == targetGUID {
			group.TargetIcons[i] = 0

			err = g.ep.TargetIconUpdated(&events.GroupEventNewTargetIconPayload{
				ServiceID: groupserver.ServiceID,
				RealmID:   realmID,
				GroupID:   group.ID,
				Updater:   0,
				Target:    0,
				IconID:    uint8(i),
				Receivers: group.OnlineMemberGUIDs(),
			})
			if err != nil {
				log.Error().Err(err).Msg("can't create TargetIconUpdated clear event")
			}

			break
		}
	}

	group.TargetIcons[iconID] = targetGUID

	if err = g.r.Update(ctx, realmID, group); err != nil {
		return fmt.Errorf("can't update icon for the group (%d), err: %w", groupID, err)
	}

	err = g.ep.TargetIconUpdated(&events.GroupEventNewTargetIconPayload{
		ServiceID: groupserver.ServiceID,
		RealmID:   realmID,
		GroupID:   group.ID,
		Updater:   updaterGUID,
		Target:    targetGUID,
		IconID:    iconID,
		Receivers: group.OnlineMemberGUIDs(),
	})
	if err != nil {
		log.Error().Err(err).Msg("can't create TargetIconUpdated event")
	}

	return nil
}

func (g groupServiceImpl) SetLootMethod(ctx context.Context, realmID uint32, updaterGUID uint64, method uint8, lootMaster uint64, lootThreshold uint8) error {
	group, err := g.getGroupWithLeader(ctx, realmID, updaterGUID)
	if err != nil {
		return err
	}

	group.LootMethod = method
	group.LootThreshold = lootThreshold
	group.LooterGUID = lootMaster

	if err = g.r.Update(ctx, realmID, group); err != nil {
		return err
	}

	err = g.ep.LootTypeChanged(&events.GroupEventGroupLootTypeChangedPayload{
		ServiceID:          groupserver.ServiceID,
		RealmID:            realmID,
		GroupID:            group.ID,
		NewLootType:        group.LootMethod,
		NewLooterGUID:      group.LooterGUID,
		NewLooterThreshold: group.LootThreshold,
		OnlineMembers:      group.OnlineMemberGUIDs(),
	})
	if err != nil {
		log.Error().Err(err).Msg("can't send loot changed event")
	}

	return nil
}

func (g groupServiceImpl) SetDungeonDifficulty(ctx context.Context, realmID uint32, updaterGUID uint64, difficulty uint8) error {
	group, err := g.getGroupWithLeader(ctx, realmID, updaterGUID)
	if err != nil {
		return err
	}

	characters, err := g.charClient.ShortOnlineCharactersDataByGUIDs(ctx, &pb.ShortCharactersDataByGUIDsRequest{
		Api:     groupserver.Ver,
		RealmID: realmID,
		GUIDs:   group.OnlineMemberGUIDs(),
	})
	if err != nil {
		return fmt.Errorf("failed to get characters, err: %w", err)
	}

	for _, char := range characters.Characters {
		if MapID(int(char.CharMap)).IsDungeon() {
			return ErrMemberInDungeonOrRaid
		}
	}

	group.Difficulty = difficulty

	if err = g.r.Update(ctx, realmID, group); err != nil {
		return err
	}

	err = g.ep.GroupDifficultyChanged(&events.GroupEventGroupDifficultyChangedPayload{
		ServiceID:         groupserver.ServiceID,
		RealmID:           realmID,
		GroupID:           group.ID,
		Updater:           updaterGUID,
		DungeonDifficulty: &difficulty,
		RaidDifficulty:    nil,
		Receivers:         group.OnlineMemberGUIDs(),
	})
	if err != nil {
		log.Error().Err(err).Msg("can't send difficulty changed event")
	}

	return nil
}

func (g groupServiceImpl) SetRaidDifficulty(ctx context.Context, realmID uint32, updaterGUID uint64, difficulty uint8) error {
	group, err := g.getGroupWithLeader(ctx, realmID, updaterGUID)
	if err != nil {
		return err
	}

	characters, err := g.charClient.ShortOnlineCharactersDataByGUIDs(ctx, &pb.ShortCharactersDataByGUIDsRequest{
		Api:     groupserver.Ver,
		RealmID: realmID,
		GUIDs:   group.OnlineMemberGUIDs(),
	})
	if err != nil {
		return fmt.Errorf("failed to get characters, err: %w", err)
	}

	for _, char := range characters.Characters {
		if MapID(int(char.CharMap)).IsRaid() {
			return ErrMemberInDungeonOrRaid
		}
	}

	group.RaidDifficulty = difficulty

	if err = g.r.Update(ctx, realmID, group); err != nil {
		return err
	}

	err = g.ep.GroupDifficultyChanged(&events.GroupEventGroupDifficultyChangedPayload{
		ServiceID:         groupserver.ServiceID,
		RealmID:           realmID,
		GroupID:           group.ID,
		Updater:           updaterGUID,
		DungeonDifficulty: nil,
		RaidDifficulty:    &difficulty,
		Receivers:         group.OnlineMemberGUIDs(),
	})
	if err != nil {
		log.Error().Err(err).Msg("can't send difficulty changed event")
	}

	return nil
}

func (g groupServiceImpl) HandleCharacterLoggedIn(payload events.GWEventCharacterLoggedInPayload) error {
	p, err := g.buildGroupMemberOnlineStatusChangedPayload(payload.RealmID, payload.CharGUID)
	if err != nil {
		return err
	}

	if p == nil {
		return nil
	}

	p.IsOnline = true
	return g.ep.GroupMemberOnlineStatusChanged(p)
}

func (g groupServiceImpl) HandleCharacterLoggedOut(payload events.GWEventCharacterLoggedOutPayload) error {
	p, err := g.buildGroupMemberOnlineStatusChangedPayload(payload.RealmID, payload.CharGUID)
	if err != nil {
		return err
	}

	if p == nil {
		return nil
	}

	p.IsOnline = false
	return g.ep.GroupMemberOnlineStatusChanged(p)
}

func (g groupServiceImpl) buildGroupMemberOnlineStatusChangedPayload(realmID uint32, player uint64) (*events.GroupEventGroupMemberOnlineStatusChangedPayload, error) {
	groupID, err := g.GroupIDByPlayer(context.Background(), realmID, player)
	if err != nil {
		return nil, err
	}

	if groupID == 0 {
		return nil, nil
	}

	group, err := g.GroupByID(context.Background(), realmID, groupID)
	if err != nil {
		return nil, err
	}

	return &events.GroupEventGroupMemberOnlineStatusChangedPayload{
		ServiceID:     groupserver.ServiceID,
		RealmID:       realmID,
		GroupID:       groupID,
		MemberGUID:    player,
		OnlineMembers: group.OnlineMemberGUIDs(),
	}, nil
}

func (g groupServiceImpl) getGroupWithLeader(ctx context.Context, realmID uint32, leaderGUID uint64) (*repo.Group, error) {
	groupID, err := g.r.GroupIDByPlayer(ctx, realmID, leaderGUID)
	if err != nil {
		return nil, fmt.Errorf("can't get groupID, err: %w", err)
	}
	if groupID == 0 {
		return nil, ErrGroupNotFound
	}

	group, err := g.r.GroupByID(ctx, realmID, groupID, true)
	if err != nil {
		return nil, fmt.Errorf("can't get group, err: %w", err)
	}

	if group == nil {
		return nil, ErrGroupNotFound
	}

	if group.LeaderGUID != leaderGUID {
		return nil, ErrNoPermissions
	}

	return group, nil
}

func (g groupServiceImpl) createGroup(ctx context.Context, realmID uint32, invite *repo.GroupInvite) error {
	group := repo.Group{
		LeaderGUID:       invite.Inviter,
		LootMethod:       uint8(repo.LootTypeFreeForAll),
		LooterGUID:       invite.Inviter,
		LootThreshold:    uint8(repo.ItemQualityUncommon),
		TargetIcons:      [8]uint64{},
		GroupType:        repo.GroupTypeFlagsNormal,
		Difficulty:       0,
		RaidDifficulty:   0,
		MasterLooterGuid: invite.Inviter,
		Members: []repo.GroupMember{
			{
				MemberGUID:  invite.Inviter,
				MemberFlags: 0,
				MemberName:  invite.InviterName,
				IsOnline:    true,
				SubGroup:    0,
				Roles:       0,
			},
			{
				MemberGUID:  invite.Invitee,
				MemberFlags: 0,
				MemberName:  invite.InviteeName,
				IsOnline:    true,
				SubGroup:    0,
				Roles:       0,
			},
		},
	}

	err := g.r.Create(ctx, realmID, &group)
	if err != nil {
		return err
	}

	members := make([]events.GroupMember, len(group.Members))
	for i, member := range group.Members {
		members[i].MemberGUID = member.MemberGUID
		members[i].MemberFlags = member.MemberFlags
		members[i].MemberName = member.MemberName
		members[i].SubGroup = member.SubGroup
		members[i].IsOnline = member.IsOnline
		members[i].Roles = uint8(member.Roles)
	}

	err = g.ep.GroupCreated(&events.GroupEventGroupCreatedPayload{
		ServiceID:        groupserver.ServiceID,
		RealmID:          realmID,
		GroupID:          group.ID,
		LeaderGUID:       group.LeaderGUID,
		LootMethod:       group.LootMethod,
		LooterGUID:       group.LooterGUID,
		LootThreshold:    group.LootThreshold,
		GroupType:        uint8(group.GroupType),
		Difficulty:       group.Difficulty,
		RaidDifficulty:   group.RaidDifficulty,
		MasterLooterGuid: group.MasterLooterGuid,
		Members:          members,
	})
	if err != nil {
		log.Error().Err(err).Msg("can't send group created event")
	}

	return nil
}

func (g groupServiceImpl) addMember(ctx context.Context, realmID uint32, group *repo.Group, invite *repo.GroupInvite) error {
	err := g.r.AddMember(ctx, realmID, &repo.GroupMember{
		GroupID:     invite.GroupID,
		MemberGUID:  invite.Invitee,
		MemberFlags: 0,
		MemberName:  invite.InviteeName,
		IsOnline:    true,
		SubGroup:    0,
		Roles:       0,
	})
	if err != nil {
		return err
	}

	err = g.ep.MemberAdded(&events.GroupEventGroupMemberAddedPayload{
		ServiceID:     groupserver.ServiceID,
		RealmID:       realmID,
		GroupID:       group.ID,
		MemberGUID:    invite.Invitee,
		MemberName:    invite.InviteeName,
		OnlineMembers: append(group.OnlineMemberGUIDs(), invite.Invitee),
	})
	if err != nil {
		log.Error().Err(err).Msg("can't send group member added event")
	}

	return nil
}

func (g groupServiceImpl) disband(ctx context.Context, realmID uint32, group *repo.Group) error {
	players := group.OnlineMemberGUIDs()
	err := g.r.Delete(ctx, realmID, group.ID)
	if err != nil {
		return fmt.Errorf("can't delete group, err: %w", err)
	}

	err = g.ep.GroupDisband(&events.GroupEventGroupDisbandPayload{
		ServiceID:     groupserver.ServiceID,
		RealmID:       realmID,
		GroupID:       group.ID,
		OnlineMembers: players,
	})
	if err != nil {
		log.Error().Err(err).Msg("can't create GroupDisband event")
	}

	return nil
}

func (g groupServiceImpl) changeLeader(ctx context.Context, realmID uint32, group *repo.Group, newLeader uint64, needsEventUpdate bool) error {
	prevLeader := group.LeaderGUID
	group.LeaderGUID = newLeader
	if err := g.r.Update(ctx, realmID, group); err != nil {
		return fmt.Errorf("can't update group win a new leader, err: %w", err)
	}
	if needsEventUpdate {
		err := g.ep.LeaderChanged(&events.GroupEventGroupLeaderChangedPayload{
			ServiceID:      groupserver.ServiceID,
			RealmID:        realmID,
			GroupID:        group.ID,
			PreviousLeader: prevLeader,
			NewLeader:      newLeader,
			OnlineMembers:  group.OnlineMemberGUIDs(),
		})
		if err != nil {
			log.Error().Err(err).Msg("can't create LeaderChanged event")
		}
	}

	return nil
}

func (g groupServiceImpl) StartReadyCheck(ctx context.Context, realmID uint32, leaderGUID uint64, durationMs uint32) error {
	if durationMs == 0 {
		durationMs = 35000
	}

	group, err := g.GroupByMemberGUID(ctx, realmID, leaderGUID)
	if err != nil {
		return err
	}
	if group == nil {
		return ErrGroupNotFound
	}

	member := group.MemberByGUID(leaderGUID)
	if member == nil {
		return ErrGroupMemberNotFound
	}

	if group.LeaderGUID != leaderGUID && !member.IsAssistant() {
		return ErrNoPermissions
	}

	receivers := group.OnlineMemberGUIDs()

	if err := g.ep.GroupReadyCheckStarted(&events.GroupEventReadyCheckStartedPayload{
		ServiceID:  groupserver.ServiceID,
		RealmID:    realmID,
		GroupID:    group.ID,
		LeaderGUID: leaderGUID,
		DurationMs: durationMs,
		Receivers:  receivers,
	}); err != nil {
		return err
	}

	go func(realmID uint32, groupID uint, receivers []uint64, durationMs uint32) {
		time.Sleep(time.Duration(durationMs) * time.Millisecond)

		_ = g.ep.GroupReadyCheckFinished(&events.GroupEventReadyCheckFinishedPayload{
			ServiceID: groupserver.ServiceID,
			RealmID:   realmID,
			GroupID:   groupID,
			Receivers: receivers,
		})
	}(realmID, group.ID, append([]uint64(nil), receivers...), durationMs)

	return nil
}

func (g groupServiceImpl) SetReadyCheckMemberState(ctx context.Context, realmID uint32, memberGUID uint64, state uint8) error {
	if state > 2 {
		state = 2
	}

	group, err := g.GroupByMemberGUID(ctx, realmID, memberGUID)
	if err != nil {
		return err
	}
	if group == nil {
		return ErrGroupNotFound
	}

	if group.MemberByGUID(memberGUID) == nil {
		return ErrGroupMemberNotFound
	}

	return g.ep.GroupReadyCheckMemberState(&events.GroupEventReadyCheckMemberStatePayload{
		ServiceID:  groupserver.ServiceID,
		RealmID:    realmID,
		GroupID:    group.ID,
		MemberGUID: memberGUID,
		State:      state,
		Receivers:  group.OnlineMemberGUIDs(),
	})
}

func (g groupServiceImpl) FinishReadyCheck(ctx context.Context, realmID uint32, playerGUID uint64) error {
	group, err := g.GroupByMemberGUID(ctx, realmID, playerGUID)
	if err != nil {
		return err
	}
	if group == nil {
		return ErrGroupNotFound
	}

	return g.ep.GroupReadyCheckFinished(&events.GroupEventReadyCheckFinishedPayload{
		ServiceID: groupserver.ServiceID,
		RealmID:   realmID,
		GroupID:   group.ID,
		Receivers: group.OnlineMemberGUIDs(),
	})
}

func (g groupServiceImpl) ChangeMemberSubGroup(ctx context.Context, realmID uint32, updaterGUID, memberGUID uint64, subGroup uint8) error {
	if subGroup >= 8 {
		return ErrNoPermissions
	}

	group, err := g.GroupByMemberGUID(ctx, realmID, updaterGUID)
	if err != nil {
		return err
	}
	if group == nil {
		return ErrGroupNotFound
	}

	updater := group.MemberByGUID(updaterGUID)
	if updater == nil {
		return ErrGroupMemberNotFound
	}

	if group.LeaderGUID != updaterGUID && !updater.IsAssistant() {
		return ErrNoPermissions
	}

	member := group.MemberByGUID(memberGUID)
	if member == nil {
		return ErrGroupMemberNotFound
	}

	if member.SubGroup == subGroup {
		return nil
	}

	if subgroupMemberCount(group, subGroup, memberGUID) >= repo.MaxGroupSize {
		return ErrGroupFull
	}

	member.SubGroup = subGroup

	if err := g.r.UpdateMember(ctx, realmID, member); err != nil {
		return err
	}

	return g.ep.GroupMemberSubGroupChanged(&events.GroupEventMemberSubGroupChangedPayload{
		ServiceID:  groupserver.ServiceID,
		RealmID:    realmID,
		GroupID:    group.ID,
		MemberGUID: memberGUID,
		SubGroup:   subGroup,
		Receivers:  group.OnlineMemberGUIDs(),
	})
}

func (g groupServiceImpl) SetMemberFlags(ctx context.Context, realmID uint32, updaterGUID, memberGUID uint64, flags, roles uint8) error {
	group, err := g.GroupByMemberGUID(ctx, realmID, updaterGUID)
	if err != nil {
		return err
	}
	if group == nil {
		return ErrGroupNotFound
	}

	updater := group.MemberByGUID(updaterGUID)
	if updater == nil {
		return ErrGroupMemberNotFound
	}

	if group.LeaderGUID != updaterGUID && !updater.IsAssistant() {
		return ErrNoPermissions
	}

	member := group.MemberByGUID(memberGUID)
	if member == nil {
		return ErrGroupMemberNotFound
	}

	if member.MemberFlags == flags && uint8(member.Roles) == roles {
		return nil
	}

	member.MemberFlags = flags
	member.Roles = repo.RoleFlags(roles)

	if err := g.r.UpdateMember(ctx, realmID, member); err != nil {
		return err
	}

	return g.ep.GroupMemberFlagsChanged(&events.GroupEventMemberFlagsChangedPayload{
		ServiceID:  groupserver.ServiceID,
		RealmID:    realmID,
		GroupID:    group.ID,
		MemberGUID: memberGUID,
		Flags:      flags,
		Roles:      roles,
		Receivers:  group.OnlineMemberGUIDs(),
	})
}

func (g groupServiceImpl) UpdateMemberState(ctx context.Context, realmID uint32, memberGUID uint64, online bool, level, class uint8, zoneID, mapID uint32, healthPct, powerPct uint16) error {
	if healthPct > 100 {
		healthPct = 100
	}
	if powerPct > 100 {
		powerPct = 100
	}

	group, err := g.GroupByMemberGUID(ctx, realmID, memberGUID)
	if err != nil {
		return err
	}
	if group == nil {
		return nil
	}

	member := group.MemberByGUID(memberGUID)
	if member == nil {
		return nil
	}

	if member.IsOnline != online {
		member.IsOnline = online

		if err := g.r.UpdateMember(ctx, realmID, member); err != nil {
			return err
		}
	}

	return g.ep.GroupMemberStateChanged(&events.GroupEventMemberStateChangedPayload{
		ServiceID:  groupserver.ServiceID,
		RealmID:    realmID,
		GroupID:    group.ID,
		MemberGUID: memberGUID,
		Online:     online,
		Level:      level,
		Class:      class,
		ZoneID:     zoneID,
		MapID:      mapID,
		HealthPct:  healthPct,
		PowerPct:   powerPct,
		Receivers:  group.OnlineMemberGUIDs(),
	})
}

func (g groupServiceImpl) ResetInstance(ctx context.Context, realmID uint32, playerGUID uint64, mapID uint32, difficulty uint8) error {
	group, err := g.GroupByMemberGUID(ctx, realmID, playerGUID)
	if err != nil {
		return err
	}

	groupID := uint(0)
	receivers := []uint64{playerGUID}

	if group != nil {
		if group.LeaderGUID != playerGUID {
			return ErrNoPermissions
		}

		groupID = group.ID
		receivers = group.OnlineMemberGUIDs()
	}

	return g.ep.GroupInstanceResetRequest(&events.GroupEventInstanceResetRequestPayload{
		ServiceID:  groupserver.ServiceID,
		RealmID:    realmID,
		GroupID:    groupID,
		PlayerGUID: playerGUID,
		MapID:      mapID,
		Difficulty: difficulty,
		Receivers:  receivers,
	})
}

func (g groupServiceImpl) SetInstanceBindExtension(ctx context.Context, realmID uint32, playerGUID uint64, mapID uint32, difficulty uint8, extended bool) error {
	group, err := g.GroupByMemberGUID(ctx, realmID, playerGUID)
	if err != nil {
		return err
	}

	groupID := uint(0)
	if group != nil {
		groupID = group.ID
	}

	return g.ep.GroupInstanceBindExtensionRequest(&events.GroupEventInstanceBindExtensionRequestPayload{
		ServiceID:  groupserver.ServiceID,
		RealmID:    realmID,
		GroupID:    groupID,
		PlayerGUID: playerGUID,
		MapID:      mapID,
		Difficulty: difficulty,
		Extended:   extended,
		Receivers:  []uint64{playerGUID},
	})
}
