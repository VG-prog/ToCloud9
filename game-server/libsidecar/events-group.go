package main

/*
#include "events-group.h"
*/
import "C"

import (
	"context"
	"fmt"
	"time"
	"unsafe"

	groupserver "github.com/walkline/ToCloud9/apps/groupserver"
	groupPb "github.com/walkline/ToCloud9/gen/group/pb"

	"github.com/rs/zerolog"

	"github.com/walkline/ToCloud9/game-server/libsidecar/consumer"
	"github.com/walkline/ToCloud9/game-server/libsidecar/queue"
	"github.com/walkline/ToCloud9/shared/events"
)

// TC9SetOnGroupCreatedHook sets hook for group created event.
//
//export TC9SetOnGroupCreatedHook
func TC9SetOnGroupCreatedHook(h C.OnGroupCreatedHook) {
	C.SetOnGroupCreatedHook(h)
}

// TC9SetOnGroupMemberAddedHook sets hook for member added event.
//
//export TC9SetOnGroupMemberAddedHook
func TC9SetOnGroupMemberAddedHook(h C.OnGroupMemberAddedHook) {
	C.SetOnGroupMemberAddedHook(h)
}

// TC9SetOnGroupMemberRemovedHook sets hook for member left/kicked event.
//
//export TC9SetOnGroupMemberRemovedHook
func TC9SetOnGroupMemberRemovedHook(h C.OnGroupMemberRemovedHook) {
	C.SetOnGroupMemberRemovedHook(h)
}

// TC9SetOnGroupDisbandedHook sets hook for group disbanded event.
//
//export TC9SetOnGroupDisbandedHook
func TC9SetOnGroupDisbandedHook(h C.OnGroupDisbandedHook) {
	C.SetOnGroupDisbandedHook(h)
}

// TC9SetOnGroupLootTypeChangedHook sets hook for group loot type changed event.
//
//export TC9SetOnGroupLootTypeChangedHook
func TC9SetOnGroupLootTypeChangedHook(h C.OnGroupLootTypeChangedHook) {
	C.SetOnGroupLootTypeChangedHook(h)
}

// TC9SetOnGroupDungeonDifficultyChangedHook sets hook for group dungeon difficulty changed event.
//
//export TC9SetOnGroupDungeonDifficultyChangedHook
func TC9SetOnGroupDungeonDifficultyChangedHook(h C.OnGroupDungeonDifficultyChangedHook) {
	C.SetOnGroupDungeonDifficultyChangedHook(h)
}

// TC9SetOnGroupRaidDifficultyChangedHook sets hook for group raid difficulty changed event.
//
//export TC9SetOnGroupRaidDifficultyChangedHook
func TC9SetOnGroupRaidDifficultyChangedHook(h C.OnGroupRaidDifficultyChangedHook) {
	C.SetOnGroupRaidDifficultyChangedHook(h)
}

// TC9SetOnGroupConvertedToRaidHook sets hook for group converted to raid event.
//
//export TC9SetOnGroupConvertedToRaidHook
func TC9SetOnGroupConvertedToRaidHook(h C.OnGroupConvertedToRaidHook) {
	C.SetOnGroupConvertedToRaidHook(h)
}

//export TC9SetOnGroupReadyCheckStartedHook
func TC9SetOnGroupReadyCheckStartedHook(h C.OnGroupReadyCheckStartedHook) {
	C.SetOnGroupReadyCheckStartedHook(h)
}

//export TC9SetOnGroupReadyCheckMemberStateHook
func TC9SetOnGroupReadyCheckMemberStateHook(h C.OnGroupReadyCheckMemberStateHook) {
	C.SetOnGroupReadyCheckMemberStateHook(h)
}

//export TC9SetOnGroupReadyCheckFinishedHook
func TC9SetOnGroupReadyCheckFinishedHook(h C.OnGroupReadyCheckFinishedHook) {
	C.SetOnGroupReadyCheckFinishedHook(h)
}

//export TC9SetOnGroupMemberSubGroupChangedHook
func TC9SetOnGroupMemberSubGroupChangedHook(h C.OnGroupMemberSubGroupChangedHook) {
	C.SetOnGroupMemberSubGroupChangedHook(h)
}

//export TC9SetOnGroupMemberFlagsChangedHook
func TC9SetOnGroupMemberFlagsChangedHook(h C.OnGroupMemberFlagsChangedHook) {
	C.SetOnGroupMemberFlagsChangedHook(h)
}

//export TC9SetOnGroupMemberStateChangedHook
func TC9SetOnGroupMemberStateChangedHook(h C.OnGroupMemberStateChangedHook) {
	C.SetOnGroupMemberStateChangedHook(h)
}

//export TC9SetOnGroupInstanceResetRequestHook
func TC9SetOnGroupInstanceResetRequestHook(h C.OnGroupInstanceResetRequestHook) {
	C.SetOnGroupInstanceResetRequestHook(h)
}

//export TC9SetOnGroupInstanceBindExtensionRequestHook
func TC9SetOnGroupInstanceBindExtensionRequestHook(h C.OnGroupInstanceBindExtensionRequestHook) {
	C.SetOnGroupInstanceBindExtensionRequestHook(h)
}

type groupHandlerFabric struct {
	logger zerolog.Logger
}

func NewGroupHandlerFabric(logger zerolog.Logger) consumer.GroupHandlersFabric {
	return &groupHandlerFabric{
		logger: logger,
	}
}

func (g groupHandlerFabric) GroupCreated(payload *events.GroupEventGroupCreatedPayload) queue.Handler {
	return eventsHandlerFunc(func() {
		cMemberGUIDs := C.malloc(C.size_t(len(payload.Members)) * C.size_t(unsafe.Sizeof(C.uint64_t(0))))
		defer C.free(cMemberGUIDs) // Make sure to free the memory when done

		cMemberGUIDsPtr := (*C.uint64_t)(cMemberGUIDs)
		for i, guid := range payload.Members {
			cMemberGUIDsPtr = (*C.uint64_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cMemberGUIDsPtr)) + uintptr(i)*unsafe.Sizeof(C.uint64_t(0))))
			*cMemberGUIDsPtr = C.uint64_t(guid.MemberGUID)
		}

		var group C.EventObjectGroup
		group.guid = C.uint32_t(payload.GroupID)
		group.leader = C.uint64_t(payload.LeaderGUID)
		group.lootMethod = C.uint8_t(payload.LootMethod)
		group.looterGuid = C.uint64_t(payload.LooterGUID)
		group.lootThreshold = C.uint8_t(payload.LootThreshold)
		group.groupType = C.uint8_t(payload.GroupType)
		group.difficulty = C.uint8_t(payload.Difficulty)
		group.raidDifficulty = C.uint8_t(payload.RaidDifficulty)
		group.masterLooterGuid = C.uint64_t(payload.MasterLooterGuid)
		group.members = (*C.uint64_t)(cMemberGUIDs)
		group.membersSize = C.uint8_t(len(payload.Members))

		r := C.CallOnGroupCreatedHook(&group)
		g.handleResponse(int(r), "GroupCreated")
	})
}

func (g groupHandlerFabric) GroupMemberAdded(payload *events.GroupEventGroupMemberAddedPayload) queue.Handler {
	return eventsHandlerFunc(func() {
		r := C.CallOnGroupMemberAddedHook(C.uint32_t(payload.GroupID), C.uint64_t(payload.MemberGUID))
		g.handleResponse(int(r), "GroupMemberAdded")
	})
}

func (g groupHandlerFabric) GroupMemberRemoved(payload *events.GroupEventGroupMemberLeftPayload) queue.Handler {
	return eventsHandlerFunc(func() {
		r := C.CallOnGroupMemberRemovedHook(C.uint32_t(payload.GroupID), C.uint64_t(payload.MemberGUID), C.uint64_t(payload.NewLeaderID))
		g.handleResponse(int(r), "GroupMemberRemoved")
	})
}

func (g groupHandlerFabric) GroupDisbanded(payload *events.GroupEventGroupDisbandPayload) queue.Handler {
	return eventsHandlerFunc(func() {
		r := C.CallOnGroupDisbandedHook(C.uint32_t(payload.GroupID))
		g.handleResponse(int(r), "GroupDisbanded")
	})
}

	_, _ = groupServiceClient.UpdateMemberState(ctx, &pb.UpdateMemberStateRequest{
		RealmID:    configuredRealmID,
func (g groupHandlerFabric) GroupLootTypeChanged(payload *events.GroupEventGroupLootTypeChangedPayload) queue.Handler {
	return eventsHandlerFunc(func() {
		r := C.CallOnGroupLootTypeChangedHook(
			C.uint32_t(payload.GroupID),
			C.uint8_t(payload.NewLootType),
			C.uint64_t(payload.NewLooterGUID),
			C.uint8_t(payload.NewLooterThreshold),
		)
		g.handleResponse(int(r), "LootTypeChanged")
	})
}

func (g groupHandlerFabric) GroupDifficultyChanged(payload *events.GroupEventGroupDifficultyChangedPayload) queue.Handler {
	return eventsHandlerFunc(func() {
		fmt.Printf("DungeonDifficulty: %p, RaidDifficulty: %p\n", payload.DungeonDifficulty, payload.RaidDifficulty)
		if payload.DungeonDifficulty != nil {
			r := C.CallOnGroupDungeonDifficultyChangedHook(
				C.uint32_t(payload.GroupID),
				C.uint8_t(*payload.DungeonDifficulty),
			)
			g.handleResponse(int(r), "DungeonDifficultyChanged")
		}

		if payload.RaidDifficulty != nil {
			r := C.CallOnGroupRaidDifficultyChangedHook(
				C.uint32_t(payload.GroupID),
				C.uint8_t(*payload.RaidDifficulty),
			)
			g.handleResponse(int(r), "RaidDifficultyChanged")
		}
	})
}

func (g groupHandlerFabric) GroupConvertedToRaid(payload *events.GroupEventGroupConvertedToRaidPayload) queue.Handler {
	return eventsHandlerFunc(func() {
		r := C.CallOnGroupConvertedToRaidHook(
			C.uint32_t(payload.GroupID),
		)
		g.handleResponse(int(r), "GroupConvertedToRaid")
	})
}

func (g groupHandlerFabric) handleResponse(resp int, hookName string) {
	const (
		CallStatusOk     = 0
		CallStatusNoHook = 1
	)
	switch resp {
	case CallStatusOk:
	case CallStatusNoHook:
		g.logger.Warn().Str("hook", hookName).Msg("no bound hook")
	default:
		g.logger.Error().Str("hook", hookName).Msg("unk status")
	}
}

func (g groupHandlerFabric) GroupReadyCheckStarted(payload *events.GroupEventReadyCheckStartedPayload) queue.Handler {
	return eventsHandlerFunc(func() {
		req := C.GroupReadyCheckStarted{
			groupGuid:  C.uint32_t(payload.GroupID),
			leaderGuid: C.uint64_t(payload.LeaderGUID),
			durationMs: C.uint32_t(payload.DurationMs),
		}

		r := C.CallOnGroupReadyCheckStartedHook(&req)
		g.handleResponse(int(r), "GroupReadyCheckStarted")
	})
}

func (g groupHandlerFabric) GroupReadyCheckMemberState(payload *events.GroupEventReadyCheckMemberStatePayload) queue.Handler {
	return eventsHandlerFunc(func() {
		req := C.GroupReadyCheckMemberState{
			groupGuid:  C.uint32_t(payload.GroupID),
			memberGuid: C.uint64_t(payload.MemberGUID),
			state:      C.uint8_t(payload.State),
		}

		r := C.CallOnGroupReadyCheckMemberStateHook(&req)
		g.handleResponse(int(r), "GroupReadyCheckMemberState")
	})
}

func (g groupHandlerFabric) GroupReadyCheckFinished(payload *events.GroupEventReadyCheckFinishedPayload) queue.Handler {
	return eventsHandlerFunc(func() {
		req := C.GroupReadyCheckFinished{
			groupGuid: C.uint32_t(payload.GroupID),
		}

		r := C.CallOnGroupReadyCheckFinishedHook(&req)
		g.handleResponse(int(r), "GroupReadyCheckFinished")
	})
}

func (g groupHandlerFabric) GroupMemberSubGroupChanged(payload *events.GroupEventMemberSubGroupChangedPayload) queue.Handler {
	return eventsHandlerFunc(func() {
		req := C.GroupMemberSubGroupChanged{
			groupGuid:  C.uint32_t(payload.GroupID),
			memberGuid: C.uint64_t(payload.MemberGUID),
			subGroup:   C.uint8_t(payload.SubGroup),
		}

		r := C.CallOnGroupMemberSubGroupChangedHook(&req)
		g.handleResponse(int(r), "GroupMemberSubGroupChanged")
	})
}

func (g groupHandlerFabric) GroupMemberFlagsChanged(payload *events.GroupEventMemberFlagsChangedPayload) queue.Handler {
	return eventsHandlerFunc(func() {
		req := C.GroupMemberFlagsChanged{
			groupGuid:  C.uint32_t(payload.GroupID),
			memberGuid: C.uint64_t(payload.MemberGUID),
			flags:      C.uint8_t(payload.Flags),
			roles:      C.uint8_t(payload.Roles),
		}

		r := C.CallOnGroupMemberFlagsChangedHook(&req)
		g.handleResponse(int(r), "GroupMemberFlagsChanged")
	})
}

func (g groupHandlerFabric) GroupMemberStateChanged(payload *events.GroupEventMemberStateChangedPayload) queue.Handler {
	return eventsHandlerFunc(func() {
		req := C.GroupMemberStateChanged{
			groupGuid:   C.uint32_t(payload.GroupID),
			memberGuid:  C.uint64_t(payload.MemberGUID),
			online:      C.uint8_t(boolToUint8(payload.Online)),
			level:       C.uint8_t(payload.Level),
			playerClass: C.uint8_t(payload.Class),
			zoneId:      C.uint32_t(payload.ZoneID),
			mapId:       C.uint32_t(payload.MapID),
			healthPct:   C.uint16_t(payload.HealthPct),
			powerPct:    C.uint16_t(payload.PowerPct),
		}

		r := C.CallOnGroupMemberStateChangedHook(&req)
		g.handleResponse(int(r), "GroupMemberStateChanged")
	})
}

func (g groupHandlerFabric) GroupInstanceResetRequest(payload *events.GroupEventInstanceResetRequestPayload) queue.Handler {
	return eventsHandlerFunc(func() {
		req := C.GroupInstanceResetRequest{
			groupGuid:  C.uint32_t(payload.GroupID),
			playerGuid: C.uint64_t(payload.PlayerGUID),
			mapId:      C.uint32_t(payload.MapID),
			difficulty: C.uint8_t(payload.Difficulty),
		}

		r := C.CallOnGroupInstanceResetRequestHook(&req)
		g.handleResponse(int(r), "GroupInstanceResetRequest")
	})
}

func (g groupHandlerFabric) GroupInstanceBindExtensionRequest(payload *events.GroupEventInstanceBindExtensionRequestPayload) queue.Handler {
	return eventsHandlerFunc(func() {
		req := C.GroupInstanceBindExtensionRequest{
			groupGuid:  C.uint32_t(payload.GroupID),
			playerGuid: C.uint64_t(payload.PlayerGUID),
			mapId:      C.uint32_t(payload.MapID),
			difficulty: C.uint8_t(payload.Difficulty),
			extended:   C.uint8_t(boolToUint8(payload.Extended)),
		}

		r := C.CallOnGroupInstanceBindExtensionRequestHook(&req)
		g.handleResponse(int(r), "GroupInstanceBindExtensionRequest")
	})
}

func boolToUint8(v bool) uint8 {
	if v {
		return 1
	}

	return 0
}
