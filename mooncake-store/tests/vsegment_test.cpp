#include "vsegment/vsegment.h"
#include "vsegment/vsegment_runtime.h"
#include "vsegment/vsegment_transfer.h"
#include "cvm/cvm_keys.h"
#include "cvm/etcd_view_store.h"
#include "vsegment/vsegment_ha.h"
#include "vsegment/vsegment_service.h"
#include "vsegment/vsegment_manager.h"
#include "vsegment/vsegment_metrics.h"
#include "vsegment/partition_quota_planner.h"
#include "master_service.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <limits>
#include <thread>

namespace mooncake::vsegment {
namespace {

struct LegacySlotMetadataExport {
    uint16_t slot{0};
    std::string source_master_id;
    std::vector<StandbyObjectEntry> objects;
};
YLT_REFL(LegacySlotMetadataExport, slot, source_master_id, objects);

struct LegacyCompletedOperationRecord {
    std::string operation_id;
    std::string allocation_id;
    OperationOutcome outcome{OperationOutcome::ABORTED};
    LogicalRange range;
};
YLT_REFL(LegacyCompletedOperationRecord, operation_id, allocation_id, outcome,
         range);

struct LegacyLogicalAllocationSnapshot {
    uint64_t logical_capacity{0};
    std::vector<LogicalRange> free_ranges;
    std::vector<ReservationRecord> reservations;
    std::vector<LegacyCompletedOperationRecord> completed_operations;
};
YLT_REFL(LegacyLogicalAllocationSnapshot, logical_capacity, free_ranges,
         reservations, completed_operations);

struct LegacyVSegmentOpLogRecord {
    std::string partition_id;
    uint64_t route_epoch{0};
    uint64_t metadata_revision{0};
    std::string mutation;
    PartitionVSegmentSnapshot state;
};
YLT_REFL(LegacyVSegmentOpLogRecord, partition_id, route_epoch,
         metadata_revision, mutation, state);

TEST(VSegmentKeysTest, PartitionQuotaSnapshotIsClusterScoped) {
    EXPECT_EQ(cvm::VSegmentPartitionQuotaSnapshotKey("cluster-a"),
              "/cvm/cluster-a/snapshot/vsegment_partition_quota");
    EXPECT_NE(cvm::VSegmentPartitionQuotaSnapshotKey("cluster-a"),
              cvm::VSegmentPartitionQuotaSnapshotKey("cluster-b"));
}

TEST(VSegmentMigrationTest, SlotExportAcceptsLegacyPayload) {
    LegacySlotMetadataExport legacy{7, "submaster-a", {}};
    auto bytes = struct_pack::serialize(legacy);
    SlotMetadataExport decoded;
    ASSERT_EQ(struct_pack::deserialize_to(decoded, bytes),
              struct_pack::errc::ok);
    EXPECT_EQ(decoded.slot, 7);
    EXPECT_EQ(decoded.source_master_id, "submaster-a");
    EXPECT_FALSE(decoded.vsegment_partition.has_value());
}

TEST(VSegmentSnapshotCompatibilityTest, ReadsLegacyCompletedOperation) {
    LegacyLogicalAllocationSnapshot legacy{
        64, {}, {}, {{"put-1", "object-1", OperationOutcome::COMMITTED,
                      {0, 64}}}};
    const auto bytes = struct_pack::serialize(legacy);
    LogicalAllocationSnapshot decoded;
    ASSERT_EQ(struct_pack::deserialize_to(decoded, bytes),
              struct_pack::errc::ok);
    ASSERT_EQ(decoded.completed_operations.size(), 1u);
    EXPECT_EQ(decoded.completed_operations[0].completion_revision.value_or(0),
              0u);
}

TEST(VSegmentRouteStoreTest, RouteSerializationRoundTrip) {
    partition::PartitionRoute route{
        {"partition-7"}, "submaster-a", 9,
        static_cast<int32_t>(partition::PartitionState::kMigrating),
        "submaster-b"};
    std::string encoded;
    ASSERT_EQ(cvm::EtcdViewStore::SerializePartitionRoute(route, encoded),
              ErrorCode::OK);
    partition::PartitionRoute decoded;
    ASSERT_EQ(cvm::EtcdViewStore::DeserializePartitionRoute(encoded, decoded),
              ErrorCode::OK);
    EXPECT_EQ(decoded.partition_id.partition_id,
              route.partition_id.partition_id);
    EXPECT_EQ(decoded.owner_submaster_id, route.owner_submaster_id);
    EXPECT_EQ(decoded.route_epoch, route.route_epoch);
    EXPECT_EQ(decoded.state, route.state);
    EXPECT_EQ(decoded.target_submaster_id, route.target_submaster_id);
}

class TestStateCommitter : public VSegmentStateCommitter {
   public:
    ErrorCode Commit(const PartitionVSegmentSnapshot& state,
                     const std::string& mutation,
                     std::string* detail) override {
        mutations.push_back(mutation);
        last_state = state;
        if (delay_create_begin && mutation == "vsegment_create_begin")
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        if (fail_next || (!fail_mutation.empty() &&
                          mutation == fail_mutation)) {
            fail_next = false;
            fail_mutation.clear();
            if (detail) *detail = "injected persistence failure";
            return ErrorCode::PERSISTENT_FAIL;
        }
        return ErrorCode::OK;
    }

    bool fail_next{false};
    bool delay_create_begin{false};
    std::string fail_mutation;
    std::vector<std::string> mutations;
    PartitionVSegmentSnapshot last_state;
};

class TestViewProvider : public VSegmentViewProvider {
   public:
    ErrorCode LoadView(const std::string& partition_id,
                       const std::string& vsegment_id, VSegmentView* output,
                       std::string*) override {
        ++loads;
        if (view.partition_id != partition_id ||
            view.vsegment_id != vsegment_id)
            return ErrorCode::SEGMENT_NOT_FOUND;
        *output = view;
        return ErrorCode::OK;
    }
    int loads{0};
    VSegmentView view;
};

class TestEndpointResolver : public SegmentEndpointResolver {
   public:
    ErrorCode ResolveLocation(const std::string& segment_id,
                              PSegmentLocation* location) override {
        location->endpoint = "endpoint://" + segment_id;
        location->base_address = base_address;
        return ErrorCode::OK;
    }
    uint64_t base_address{4096};
};

std::shared_ptr<TestStateCommitter> Committer() {
    return std::make_shared<TestStateCommitter>();
}

VSegmentProfile Profile(uint32_t members = 2) {
    return {.name = "default",
            .member_count = members,
            .stripe_size = 64,
            .member_extent_size = 256,
            .io_alignment = 8,
            .required_medium = "DRAM"};
}

PartitionVSegmentConfig Config() {
    return {.partition_id = "partition-1",
            .profile_name = "default",
            .initial_vsegment_count = 1,
            .config_generation = 1,
            .quotas = {{"segment-a", 0, 512}, {"segment-b", 1024, 512}}};
}

VSegmentView View() {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    return allocation.view;
}

TEST(VSegmentConfigTest, RejectsInvalidLayoutAndOverlappingQuota) {
    auto profile = Profile();
    profile.member_extent_size = 100;
    EXPECT_EQ(ValidateProfile(profile), ErrorCode::INVALID_PARAMS);

    auto config = Config();
    config.quotas.push_back({"segment-a", 128, 64});
    EXPECT_EQ(ValidatePartitionConfig(config), ErrorCode::INVALID_PARAMS);
}

TEST(VSegmentConfigTest, ValidatesPublishedConfigAgainstSegmentGeometry) {
    auto profile = Profile();
    auto config = Config();
    std::vector<PSegmentGeometry> segments = {
        {"segment-a", 2048, 0, 8, "DRAM", true, true, ""},
        {"segment-b", 2048, 0, 8, "DRAM", true, true, ""}};
    EXPECT_EQ(ValidatePublishedConfig({profile}, {config}, segments, {}),
              ErrorCode::OK);

    auto overlapping = config;
    overlapping.partition_id = "partition-2";
    overlapping.config_generation = 2;
    EXPECT_EQ(ValidatePublishedConfig({profile}, {config, overlapping},
                                      segments, {}),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(ValidatePublishedConfig({profile}, {config}, segments,
                                      {{"partition-1", 1}}),
              ErrorCode::INVALID_PARAMS);
}

TEST(PartitionQuotaPlannerTest, SplitsEveryMediumAcrossAllPartitions) {
    PartitionQuotaPlanRequest request;
    request.config_generation = 7;
    request.policy_digest = "policy-v7";
    request.default_profile = "dram";
    request.partition_ids = {"partition-b", "partition-a"};
    request.profile_specs = {
        {.name = "dram",
         .member_count = 2,
         .stripe_size = 64,
         .member_extent_size = 128,
         .io_alignment = 8,
         .required_medium = "DRAM"},
        {.name = "nvme",
         .member_count = 2,
         .stripe_size = 64,
         .member_extent_size = 128,
         .io_alignment = 8,
         .required_medium = "NVMe"}};
    request.segments = {{"dram-a", 1024, 0, 8, "DRAM", true, true, ""},
                        {"dram-b", 1024, 0, 8, "DRAM", true, true, ""},
                        {"nvme-a", 2048, 0, 8, "NVMe", true, true, ""},
                        {"nvme-b", 2048, 0, 8, "NVMe", true, true, ""}};

    auto result = PartitionQuotaPlanner().Plan(request);
    ASSERT_TRUE(result) << result.detail;
    ASSERT_EQ(result.snapshot.quotas.size(), 4);
    EXPECT_EQ(result.snapshot.quotas[0].partition_id, "partition-a");
    EXPECT_EQ(result.snapshot.quotas[0].profile_name, "dram");
    // partition_ids = {"partition-b", "partition-a"}: partition-b 是 index 0
    // (base_offset=0), partition-a 是 index 1 (base_offset=512). snapshot.quotas
    // 按 partition_id 字典序排列，partition-a 在前。
    EXPECT_EQ(result.snapshot.quotas[0].extents[0].base_offset, 512);
    EXPECT_EQ(result.snapshot.quotas[1].extents[0].base_offset, 0);
    EXPECT_EQ(result.snapshot.quotas[2].profile_name, "nvme");
    EXPECT_EQ(ValidateQuotaSnapshot(result.snapshot, request.segments),
              ErrorCode::OK);

    PartitionVSegmentConfig config;
    EXPECT_EQ(BuildPartitionConfig(result.snapshot, "partition-a", "nvme",
                                   &config),
              ErrorCode::OK);
    EXPECT_EQ(config.quotas.size(), 2);
    EXPECT_EQ(config.config_generation, 7);
}

TEST(PartitionQuotaPlannerTest, RejectsInsufficientMemberSegments) {
    PartitionQuotaPlanRequest request;
    request.config_generation = 1;
    request.policy_digest = "policy";
    request.default_profile = "default";
    request.partition_ids = {"partition-a"};
    request.profile_specs = {Profile(2)};
    request.segments = {{"only-one", 4096, 0, 8, "DRAM", true, true, ""}};
    auto result = PartitionQuotaPlanner().Plan(request);
    EXPECT_EQ(result.error, ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT);
    EXPECT_NE(result.detail.find("requires 2 psegments"), std::string::npos);
}

TEST(VSegmentConfigTest, AllowsMultipleProfilesForOnePartition) {
    auto dram = Profile();
    dram.name = "dram";
    auto nvme = Profile();
    nvme.name = "nvme";
    nvme.required_medium = "NVMe";
    auto dram_config = Config();
    dram_config.profile_name = "dram";
    auto nvme_config = Config();
    nvme_config.profile_name = "nvme";
    nvme_config.quotas = {{"nvme-a", 0, 512}, {"nvme-b", 0, 512}};
    std::vector<PSegmentGeometry> segments = {
        {"segment-a", 2048, 0, 8, "DRAM", true, true, ""},
        {"segment-b", 2048, 0, 8, "DRAM", true, true, ""},
        {"nvme-a", 2048, 0, 8, "NVMe", true, true, ""},
        {"nvme-b", 2048, 0, 8, "NVMe", true, true, ""}};
    EXPECT_EQ(ValidatePublishedConfig({dram, nvme},
                                      {dram_config, nvme_config}, segments,
                                      {}),
              ErrorCode::OK);
}

TEST(PartitionQuotaPlannerTest, RejectsOverlappingBalancedProfilePools) {
    PartitionQuotaPlanRequest request;
    request.config_generation = 1;
    request.policy_digest = "policy";
    request.default_profile = "first";
    request.partition_ids = {"partition-1"};
    auto first = Profile();
    first.name = "first";
    auto second = Profile();
    second.name = "second";
    request.profile_specs = {first, second};
    request.segments = {{"segment-a", 1024, 0, 8, "DRAM", true, true, ""},
                        {"segment-b", 1024, 0, 8, "DRAM", true, true, ""}};
    auto result = PartitionQuotaPlanner().Plan(request);
    EXPECT_EQ(result.error, ErrorCode::INVALID_PARAMS);
    EXPECT_NE(result.detail.find("overlap"), std::string::npos);
}

TEST(PartitionQuotaPlannerTest, ExcludesUnhealthySegments) {
    PartitionQuotaPlanRequest request;
    request.config_generation = 1;
    request.policy_digest = "policy";
    request.default_profile = "default";
    request.partition_ids = {"partition-a"};
    request.profile_specs = {Profile(2)};
    request.segments = {{"healthy", 4096, 0, 8, "DRAM", true, true, "host-a"},
                        {"unhealthy", 4096, 0, 8, "DRAM", false, true,
                         "host-b"}};
    EXPECT_EQ(PartitionQuotaPlanner().Plan(request).error,
              ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT);
}

TEST(PartitionQuotaPlannerTest, AppliesReservedRatioAndAlignment) {
    PartitionQuotaPlanRequest request;
    request.config_generation = 2;
    request.policy_digest = "policy";
    request.default_profile = "default";
    request.partition_ids = {"partition-b", "partition-a"};
    request.profile_specs = {Profile(2)};
    request.segments = {{"segment-a", 1000, 0, 16, "DRAM", true, true, ""},
                        {"segment-b", 1000, 0, 16, "DRAM", true, true, ""}};
    request.reserved_ratio = 0.1;

    auto result = PartitionQuotaPlanner().Plan(request);
    ASSERT_TRUE(result) << result.detail;
    ASSERT_EQ(result.snapshot.quotas.size(), 2u);
    for (const auto& quota : result.snapshot.quotas) {
        ASSERT_EQ(quota.extents.size(), 2u);
        for (const auto& extent : quota.extents) {
            EXPECT_EQ(extent.length, 448u);
            EXPECT_EQ(extent.base_offset % 16, 0u);
        }
    }
    EXPECT_EQ(result.snapshot.quotas[0].partition_id, "partition-a");
    // partition_ids = {"partition-b", "partition-a"}: partition-b 是 index 0
    // (base_offset=0), partition-a 是 index 1 (base_offset=448).
    EXPECT_EQ(result.snapshot.quotas[0].extents[0].base_offset, 448u);
    EXPECT_EQ(result.snapshot.quotas[1].partition_id, "partition-b");
    EXPECT_EQ(result.snapshot.quotas[1].extents[0].base_offset, 0u);
}

TEST(VSegmentViewTest, ChecksumCoversOrderedMembers) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    EXPECT_EQ(ValidateView(allocation.view, Profile()), ErrorCode::OK);

    std::swap(allocation.view.members[0], allocation.view.members[1]);
    EXPECT_EQ(ValidateView(allocation.view, Profile()),
              ErrorCode::CHECKSUM_MISMATCH);
}

TEST(VSegmentViewTest, LifecycleDoesNotChangeImmutableViewChecksum) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    VSegmentStateSnapshot state{"default", Lifecycle::ACTIVE,
                                allocation.view, {}};
    const auto checksum = state.view.checksum;
    state.lifecycle = Lifecycle::DRAINING;
    EXPECT_EQ(state.view.checksum, checksum);
    EXPECT_EQ(ValidateView(state.view, Profile()), ErrorCode::OK);
}

TEST(PartitionQuotaAllocatorTest, AllocatesAndReleasesStaticQuota) {
    PartitionQuotaAllocator allocator(Config());
    auto first = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(first);
    ASSERT_EQ(first.view.members.size(), 2);
    EXPECT_EQ(first.view.members[0],
              (PSegmentExtent{"segment-a", 0, 256}));
    EXPECT_EQ(first.view.members[1],
              (PSegmentExtent{"segment-b", 1024, 256}));
    EXPECT_EQ(allocator.FreeBytes("segment-a"), 256);

    EXPECT_EQ(allocator.Release(first.view), ErrorCode::OK);
    EXPECT_EQ(allocator.FreeBytes("segment-a"), 512);
    EXPECT_EQ(allocator.FreeBytes("segment-b"), 512);
}

TEST(PartitionQuotaAllocatorTest, DoesNotDegradeConfiguredMemberCount) {
    auto config = Config();
    config.quotas.pop_back();
    PartitionQuotaAllocator allocator(config);
    auto allocation = allocator.Allocate("vs-1", Profile());
    EXPECT_EQ(allocation.error,
              ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT);
    EXPECT_NE(allocation.detail.find("requires 2 psegments"),
              std::string::npos);
    EXPECT_EQ(allocator.FreeBytes("segment-a"), 512);
}

TEST(PartitionQuotaAllocatorTest, ConcurrentIdentityIsIdempotentlyRejected) {
    PartitionQuotaAllocator allocator(Config());
    ASSERT_TRUE(allocator.Allocate("vs-1", Profile()));
    auto duplicate = allocator.Allocate("vs-1", Profile());
    EXPECT_EQ(duplicate.error, ErrorCode::SEGMENT_ALREADY_EXISTS);
}

TEST(LogicalRangeAllocatorTest, ReservationIsIdempotentAndAbortReturnsSpace) {
    LogicalRangeAllocator allocator(1024);
    auto first = allocator.Reserve("put-1", 128);
    ASSERT_TRUE(first);
    auto retry = allocator.Reserve("put-1", 128);
    ASSERT_TRUE(retry);
    EXPECT_EQ(retry.range.offset, first.range.offset);
    EXPECT_EQ(allocator.FreeBytes(), 896);
    EXPECT_EQ(allocator.ReservationCount(), 1);
    EXPECT_EQ(allocator.Abort("put-1"), ErrorCode::OK);
    EXPECT_EQ(allocator.Abort("put-1"), ErrorCode::OK);
    EXPECT_EQ(allocator.FreeBytes(), 1024);
}

TEST(LogicalRangeAllocatorTest, ReleaseRequiresExactObjectAllocation) {
    LogicalRangeAllocator allocator(1024);
    ASSERT_TRUE(allocator.Reserve("put-1", 128));
    LogicalRange committed;
    ASSERT_EQ(allocator.Commit("put-1", "object-1", &committed),
              ErrorCode::OK);
    EXPECT_EQ(allocator.Release("object-2", committed),
              ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_EQ(allocator.Release("object-1",
                                {committed.offset, committed.length / 2}),
              ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_EQ(allocator.FreeBytes(), 896);
}

TEST(LogicalRangeAllocatorTest, CommitKeepsRangeAllocatedUntilRelease) {
    LogicalRangeAllocator allocator(1024);
    ASSERT_TRUE(allocator.Reserve("put-1", 128));
    LogicalRange committed;
    EXPECT_EQ(allocator.Commit("put-1", "object-1", &committed),
              ErrorCode::OK);
    LogicalRange retried;
    EXPECT_EQ(allocator.Commit("put-1", "object-1", &retried),
              ErrorCode::OK);
    EXPECT_EQ(retried.offset, committed.offset);
    EXPECT_EQ(allocator.FreeBytes(), 896);
    EXPECT_EQ(allocator.Release("object-1", committed), ErrorCode::OK);
    EXPECT_EQ(allocator.FreeBytes(), 1024);
    EXPECT_EQ(allocator.Release("object-1", committed),
              ErrorCode::OBJECT_NOT_FOUND);
}

TEST(LogicalRangeAllocatorTest, RejectsLengthChangeOnIdempotentRetry) {
    LogicalRangeAllocator allocator(1024);
    ASSERT_TRUE(allocator.Reserve("put-1", 128));
    EXPECT_EQ(allocator.Reserve("put-1", 64).error,
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(allocator.FreeBytes(), 896u);
    EXPECT_EQ(allocator.ReservationCount(), 1u);
}

TEST(LogicalRangeAllocatorTest, InternalRollbackAllowsOperationRetry) {
    LogicalRangeAllocator allocator(128);
    auto first = allocator.Reserve("put-1", 64);
    ASSERT_TRUE(first);
    ASSERT_EQ(allocator.CancelReservation("put-1"), ErrorCode::OK);
    auto retried = allocator.Reserve("put-1", 64);
    ASSERT_TRUE(retried);
    EXPECT_EQ(retried.range.offset, first.range.offset);
}

TEST(VSegmentResolverTest, SplitsAtStripeAndClientSliceBoundaries) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);

    auto resolved = ResolveTransfer(allocation.view, 48, 96,
                                    {{1000, 40}, {2000, 56}});
    ASSERT_TRUE(resolved);
    ASSERT_EQ(resolved.requests.size(), 4);
    EXPECT_EQ(resolved.requests[0].length, 16);
    EXPECT_EQ(resolved.requests[0].segment_id, "segment-a");
    EXPECT_EQ(resolved.requests[0].physical_offset, 48);
    EXPECT_EQ(resolved.requests[1].length, 24);
    EXPECT_EQ(resolved.requests[1].segment_id, "segment-b");
    EXPECT_EQ(resolved.requests[1].client_address, 1016);
    EXPECT_EQ(resolved.requests[2].length, 40);
    EXPECT_EQ(resolved.requests[2].client_address, 2000);
    EXPECT_EQ(resolved.requests[3].length, 16);
    EXPECT_EQ(resolved.requests[3].segment_id, "segment-a");
    EXPECT_EQ(resolved.requests[3].physical_offset, 64);
    EXPECT_EQ(resolved.requests[3].client_buffer_offset, 80);
}

TEST(VSegmentResolverTest, RejectsViewWithInvalidChecksum) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    allocation.view.members[0].base_offset += 8;
    auto resolved = ResolveTransfer(allocation.view, 0, 8, {{1000, 8}});
    EXPECT_EQ(resolved.error, ErrorCode::CHECKSUM_MISMATCH);
}

TEST(VSegmentResolverTest, RejectsBufferLengthMismatchAndLogicalOverflow) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);

    EXPECT_EQ(ResolveTransfer(allocation.view, 0, 16, {{1000, 8}}).error,
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(ResolveTransfer(allocation.view,
                              allocation.view.logical_capacity - 8, 16,
                              {{1000, 16}})
                  .error,
              ErrorCode::INVALID_PARAMS);
}

TEST(VSegmentTransferPlannerTest, LoadsImmutableViewAndResolvesEndpoints) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    TestViewProvider provider;
    provider.view = allocation.view;
    TestEndpointResolver endpoints;
    VSegmentViewCache cache;
    VSegmentTransferPlanner planner(&provider, &endpoints, &cache);
    VSegmentDescriptor replica{"partition-1", "vs-1", 48, 96};

    auto first = planner.Plan(replica, {{1000, 96}});
    ASSERT_TRUE(first) << first.detail;
    ASSERT_EQ(first.requests.size(), 3);
    EXPECT_EQ(first.requests[0].endpoint, "endpoint://segment-a");
    EXPECT_EQ(first.requests[0].transfer.physical_offset, 4096 + 48);
    EXPECT_EQ(first.requests[1].endpoint, "endpoint://segment-b");
    EXPECT_EQ(provider.loads, 1);

    auto cached = planner.Plan(replica, {{2000, 96}});
    ASSERT_TRUE(cached);
    EXPECT_EQ(provider.loads, 1);
}

TEST(VSegmentTransferPlannerTest, RejectsAbsoluteAddressOverflow) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    TestViewProvider provider;
    provider.view = allocation.view;
    TestEndpointResolver endpoints;
    endpoints.base_address = std::numeric_limits<uint64_t>::max();
    VSegmentViewCache cache;
    VSegmentTransferPlanner planner(&provider, &endpoints, &cache);
    VSegmentDescriptor replica{"partition-1", "vs-1", 1, 8};
    EXPECT_EQ(planner.Plan(replica, {{1000, 8}}).error,
              ErrorCode::INVALID_PARAMS);
}

TEST(VSegmentTransferPlannerTest, RejectsConflictingCachedView) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    VSegmentViewCache cache;
    ASSERT_EQ(cache.Insert(allocation.view), ErrorCode::OK);
    auto conflicting = allocation.view;
    conflicting.members[0].base_offset += 8;
    conflicting.checksum = ComputeViewChecksum(conflicting);
    EXPECT_EQ(cache.Insert(conflicting), ErrorCode::INVALID_VERSION);
}

TEST(VSegmentViewCacheTest, ComparesLayoutInsteadOfTrustingChecksum) {
    auto original = View();
    VSegmentViewCache cache;
    ASSERT_EQ(cache.Insert(original), ErrorCode::OK);

    auto conflicting = original;
    conflicting.members[0].base_offset += original.stripe_size;
    // Simulate a checksum collision or incorrectly copied checksum. Structure
    // remains valid because checksum still matches the supplied value only
    // after deliberately retaining the original checksum.
    conflicting.checksum = original.checksum;
    EXPECT_EQ(cache.Insert(conflicting), ErrorCode::CHECKSUM_MISMATCH);

    conflicting.checksum = ComputeViewChecksum(conflicting);
    EXPECT_EQ(cache.Insert(conflicting), ErrorCode::INVALID_VERSION);
}

TEST(VSegmentViewCacheTest, ScopesIdentityByPartition) {
    auto first = View();
    auto second = first;
    second.partition_id = "partition-2";
    second.stripe_size *= 2;
    second.checksum = ComputeViewChecksum(second);

    VSegmentViewCache cache;
    ASSERT_EQ(cache.Insert(first), ErrorCode::OK);
    ASSERT_EQ(cache.Insert(second), ErrorCode::OK);
    VSegmentView loaded;
    ASSERT_TRUE(cache.Find(first.partition_id, first.vsegment_id, &loaded));
    EXPECT_EQ(loaded.stripe_size, first.stripe_size);
    ASSERT_TRUE(cache.Find(second.partition_id, second.vsegment_id, &loaded));
    EXPECT_EQ(loaded.stripe_size, second.stripe_size);
}

TEST(VSegmentViewCacheTest, EvictsOldestViewAtCapacity) {
    VSegmentViewCache cache(2);
    auto first = View();
    auto second = first;
    second.vsegment_id = "vs-2";
    second.checksum = ComputeViewChecksum(second);
    auto third = first;
    third.vsegment_id = "vs-3";
    third.checksum = ComputeViewChecksum(third);

    ASSERT_EQ(cache.Insert(first), ErrorCode::OK);
    ASSERT_EQ(cache.Insert(second), ErrorCode::OK);
    ASSERT_EQ(cache.Insert(third), ErrorCode::OK);
    EXPECT_EQ(cache.Size(), 2u);
    VSegmentView loaded;
    EXPECT_FALSE(cache.Find(first.partition_id, first.vsegment_id, &loaded));
    EXPECT_TRUE(cache.Find(second.partition_id, second.vsegment_id, &loaded));
    EXPECT_TRUE(cache.Find(third.partition_id, third.vsegment_id, &loaded));
}

TEST(VSegmentViewTest, KeepsProfileIdentityOutsideImmutableView) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-profile", Profile());
    ASSERT_TRUE(allocation);
    EXPECT_EQ(ValidateView(allocation.view, Profile()), ErrorCode::OK);

    // A View describes only mapping. The owning manager snapshot keeps the
    // profile identity needed for quota recovery.
    auto wrong_profile = Profile();
    wrong_profile.name = "other";
    EXPECT_EQ(ValidateView(allocation.view, wrong_profile),
              ErrorCode::OK);
}

TEST(VSegmentReplicaTest, RuntimeMetadataPreservesLogicalDescriptor) {
    VSegmentDescriptor expected{"partition-1", "vs-1", 128, 64};
    Replica replica(expected, ReplicaStatus::COMPLETE);

    EXPECT_EQ(replica.type(), ReplicaType::VSEGMENT);
    EXPECT_TRUE(replica.is_vsegment_replica());
    const auto descriptor = replica.get_descriptor();
    ASSERT_TRUE(descriptor.is_vsegment_replica());
    const auto& actual = descriptor.get_vsegment_descriptor();
    EXPECT_EQ(actual.partition_id, expected.partition_id);
    EXPECT_EQ(actual.vsegment_id, expected.vsegment_id);
    EXPECT_EQ(actual.logical_offset, expected.logical_offset);
    EXPECT_EQ(actual.length, expected.length);
}

TEST(PartitionQuotaAllocatorTest, PreservesConfiguredMemberOrder) {
    auto config = Config();
    std::swap(config.quotas[0], config.quotas[1]);
    PartitionQuotaAllocator allocator(config);
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    EXPECT_EQ(allocation.view.members[0].segment_id, "segment-b");
    EXPECT_EQ(allocation.view.members[1].segment_id, "segment-a");
}

TEST(CreationCoordinatorTest, ConcurrentCallersShareOneCreation) {
    CreationCoordinator coordinator;
    std::atomic<int> calls{0};
    std::promise<void> factory_entered;
    std::promise<void> release_factory;
    auto release = release_factory.get_future().share();
    std::atomic<bool> second_started{false};
    VSegmentAllocationResult first;
    VSegmentAllocationResult second;
    auto factory = [&] {
        ++calls;
        factory_entered.set_value();
        release.wait();
        VSegmentAllocationResult result;
        result.view.vsegment_id = "vs-1";
        return result;
    };
    std::thread one([&] { first = coordinator.GetOrCreate("p/default/0", factory); });
    factory_entered.get_future().wait();
    std::thread two([&] {
        second_started = true;
        second = coordinator.GetOrCreate("p/default/0", factory);
    });
    while (!second_started.load()) std::this_thread::yield();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    release_factory.set_value();
    one.join();
    two.join();
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(first.view.vsegment_id, "vs-1");
    EXPECT_EQ(second.view.vsegment_id, "vs-1");
}

TEST(VSegmentManagerTest, SnapshotRestorePreservesReservationsAndFreeSpace) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    auto reservation = manager.ReservePut(allocation.view.vsegment_id,
                                          "put-1", 128);
    ASSERT_TRUE(reservation);

    const auto snapshot = manager.Snapshot();
    ASSERT_EQ(snapshot.vsegments.size(), 1);
    VSegmentManager recovered(Config(), {Profile()}, Committer());
    std::string detail;
    ASSERT_EQ(recovered.Restore(snapshot, &detail), ErrorCode::OK) << detail;

    VSegmentView restored_view;
    EXPECT_TRUE(recovered.FindView(allocation.view.vsegment_id,
                                   &restored_view));
    EXPECT_EQ(restored_view.checksum, allocation.view.checksum);
    LogicalRange committed;
    EXPECT_EQ(recovered.CommitPut(allocation.view.vsegment_id, "put-1",
                                  "object-1", &committed),
              ErrorCode::OK);
    EXPECT_EQ(committed.offset, reservation.range.offset);
    EXPECT_EQ(committed.length, reservation.range.length);
}

TEST(VSegmentManagerTest, RejectsSnapshotFromAnotherConfigGeneration) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    ASSERT_TRUE(manager.Create("default"));
    auto snapshot = manager.Snapshot();
    snapshot.config_generation = 2;

    VSegmentManager recovered(Config(), {Profile()}, Committer());
    EXPECT_EQ(recovered.Restore(snapshot), ErrorCode::INVALID_VERSION);
}

TEST(VSegmentManagerTest, EnforcesLifecycleTransitions) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    EXPECT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::RETIRED),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    EXPECT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::DRAINING),
              ErrorCode::OK);
    EXPECT_FALSE(manager.ReservePut(allocation.view.vsegment_id, "put-1", 8));
    EXPECT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::RETIRED),
              ErrorCode::OK);
}

TEST(VSegmentManagerTest, RefusesToRetireReferencedVSegment) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    ASSERT_TRUE(manager.ReservePut(allocation.view.vsegment_id, "put-1", 64));
    ASSERT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::DRAINING),
              ErrorCode::OK);
    EXPECT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::RETIRED),
              ErrorCode::OBJECT_REPLICA_BUSY);
}

TEST(VSegmentManagerTest, RetireReturnsExtentsToPartitionQuota) {
    auto config = Config();
    config.quotas[0].length = 256;
    config.quotas[1].length = 256;
    VSegmentManager manager(config, {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    EXPECT_EQ(manager.Create("default").error,
              ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT);
    ASSERT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::DRAINING),
              ErrorCode::OK);
    ASSERT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::RETIRED),
              ErrorCode::OK);
    EXPECT_TRUE(manager.Create("default"));
}

TEST(VSegmentManagerTest, ConsumesMultipleProfilesFromPublishedSnapshot) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 3;
    snapshot.policy_digest = "policy-v3";
    snapshot.default_profile = "dram";
    auto dram = Profile();
    dram.name = "dram";
    auto nvme = Profile();
    nvme.name = "nvme";
    nvme.required_medium = "NVMe";
    snapshot.profile_specs = {dram, nvme};
    snapshot.quotas = {
        {"partition-1", "dram", "DRAM",
         {{"dram-a", 0, 256}, {"dram-b", 0, 256}}},
        {"partition-1", "nvme", "NVMe",
         {{"nvme-a", 0, 256}, {"nvme-b", 0, 256}}}};

    VSegmentManager manager(snapshot, "partition-1", Committer());
    auto dram_view = manager.Create("dram");
    auto nvme_view = manager.Create("nvme");
    ASSERT_TRUE(dram_view);
    ASSERT_TRUE(nvme_view);
    EXPECT_EQ(dram_view.view.members[0].segment_id, "dram-a");
    EXPECT_EQ(nvme_view.view.members[0].segment_id, "nvme-a");
    EXPECT_EQ(manager.Snapshot().vsegments.size(), 2);
}

TEST(VSegmentManagerTest, RestoresCommittedIdentityForExactRelease) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    ASSERT_TRUE(manager.ReservePut(allocation.view.vsegment_id, "put-1", 64));
    LogicalRange range;
    ASSERT_EQ(manager.CommitPut(allocation.view.vsegment_id, "put-1",
                                "object-1", &range),
              ErrorCode::OK);
    auto snapshot = manager.Snapshot();

    VSegmentManager recovered(Config(), {Profile()}, Committer());
    ASSERT_EQ(recovered.Restore(snapshot), ErrorCode::OK);
    EXPECT_EQ(recovered.ReleaseObject(allocation.view.vsegment_id,
                                      "object-1", range),
              ErrorCode::OK);
    const auto released = recovered.Snapshot();
    ASSERT_EQ(released.vsegments.size(), 1u);
    EXPECT_TRUE(released.operation_vsegments.empty());
    EXPECT_TRUE(
        released.vsegments.front().logical_allocation.completed_operations
            .empty());
}

TEST(VSegmentManagerTest, SnapshotIsJsonSerializable) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    ASSERT_TRUE(manager.ReservePut(allocation.view.vsegment_id, "put-1", 64));

    std::string json;
    struct_json::to_json(manager.Snapshot(), json);
    PartitionVSegmentSnapshot decoded;
    struct_json::from_json(decoded, json);
    ASSERT_EQ(decoded.vsegments.size(), 1);
    EXPECT_EQ(decoded.partition_id, "partition-1");
    EXPECT_EQ(decoded.vsegments[0].logical_allocation.reservations.size(), 1);
}

TEST(VSegmentManagerTest, PersistsCreationBeforePublishingActive) {
    auto committer = Committer();
    VSegmentManager manager(Config(), {Profile()}, committer);
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    ASSERT_EQ(committer->mutations.size(), 2);
    EXPECT_EQ(committer->mutations[0], "vsegment_create_begin");
    EXPECT_EQ(committer->mutations[1], "vsegment_create_commit");
    ASSERT_EQ(committer->last_state.vsegments.size(), 1);
    EXPECT_EQ(committer->last_state.vsegments[0].lifecycle,
              Lifecycle::ACTIVE);
}

TEST(VSegmentManagerTest, RollsBackReservationWhenPersistenceFails) {
    auto committer = Committer();
    VSegmentManager manager(Config(), {Profile()}, committer);
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    committer->fail_next = true;
    auto failed = manager.ReservePut(allocation.view.vsegment_id, "put-1", 64);
    EXPECT_EQ(failed.error, ErrorCode::PERSISTENT_FAIL);

    auto retry = manager.ReservePut(allocation.view.vsegment_id, "put-1", 64);
    ASSERT_TRUE(retry);
    EXPECT_EQ(retry.range.offset, 0);
}

TEST(VSegmentManagerTest, RecoveryRollsBackUncommittedCreation) {
    auto committer = Committer();
    VSegmentManager manager(Config(), {Profile()}, committer);
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    auto snapshot = manager.Snapshot();
    snapshot.vsegments[0].lifecycle = Lifecycle::PREPARING;

    VSegmentManager recovered(Config(), {Profile()}, Committer());
    ASSERT_EQ(recovered.Restore(snapshot), ErrorCode::OK);
    VSegmentView view;
    EXPECT_FALSE(recovered.FindView(allocation.view.vsegment_id, &view));
    EXPECT_TRUE(recovered.Create("default"));
}

TEST(VSegmentManagerTest, ConcurrentProfileCreationSharesOneVSegment) {
    auto committer = Committer();
    committer->delay_create_begin = true;
    VSegmentManager manager(Config(), {Profile()}, committer);
    constexpr size_t kCallers = 8;
    std::barrier start(static_cast<std::ptrdiff_t>(kCallers));
    std::vector<VSegmentAllocationResult> results(kCallers);
    std::vector<std::thread> threads;
    for (size_t index = 0; index < kCallers; ++index) {
        threads.emplace_back([&, index] {
            start.arrive_and_wait();
            results[index] = manager.Create("default");
        });
    }
    for (auto& thread : threads) thread.join();
    for (const auto& result : results) {
        ASSERT_TRUE(result);
        EXPECT_EQ(result.view.vsegment_id, results[0].view.vsegment_id);
    }
    EXPECT_EQ(std::count(committer->mutations.begin(),
                         committer->mutations.end(),
                         "vsegment_create_begin"),
              1);
}

TEST(VSegmentManagerTest, AsyncCreationReturnsRetryableStatus) {
    auto committer = Committer();
    committer->delay_create_begin = true;
    VSegmentManager manager(Config(), {Profile()}, committer);
    auto first = manager.RequestCreate("default", 25);
    EXPECT_EQ(first.error, ErrorCode::VSEGMENT_CREATING);
    EXPECT_NE(first.detail.find("retry_after_ms=25"), std::string::npos);

    VSegmentAllocationResult completed;
    for (int attempt = 0; attempt < 20; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        completed = manager.RequestCreate("default");
        if (completed.error != ErrorCode::VSEGMENT_CREATING) break;
    }
    ASSERT_TRUE(completed) << completed.detail;
    EXPECT_EQ(std::count(committer->mutations.begin(),
                         committer->mutations.end(),
                         "vsegment_create_begin"),
              1);
}

TEST(VSegmentManagerTest, PutStartCreatesThenReturnsIdempotentDescriptor) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto creating = manager.StartPut("put-1", 96);
    EXPECT_EQ(creating.error, ErrorCode::VSEGMENT_CREATING);

    VSegmentPutStartResult started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        started = manager.StartPut("put-1", 96);
        if (started.error != ErrorCode::VSEGMENT_CREATING) break;
    }
    ASSERT_TRUE(started) << started.detail;
    EXPECT_EQ(started.replica.partition_id, "partition-1");
    EXPECT_EQ(started.replica.logical_offset, 0);
    EXPECT_EQ(started.replica.length, 96);

    auto retry = manager.StartPut("put-1", 96);
    ASSERT_TRUE(retry);
    EXPECT_EQ(retry.replica.vsegment_id, started.replica.vsegment_id);
    EXPECT_EQ(retry.replica.logical_offset, started.replica.logical_offset);
}

TEST(VSegmentManagerTest, FencesStalePartitionOwnerRequests) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    ASSERT_EQ(manager.SetRouteEpoch(7), ErrorCode::OK);
    auto stale = manager.StartPut("put-1", 64, "default", 6);
    EXPECT_EQ(stale.error, ErrorCode::STALE_ROUTE);
    auto current = manager.StartPut("put-1", 64, "default", 7);
    EXPECT_EQ(current.error, ErrorCode::VSEGMENT_CREATING);
    EXPECT_EQ(manager.SetRouteEpoch(6), ErrorCode::STALE_ROUTE);
}

TEST(VSegmentManagerTest, ExposesLifecycleStatsAndGcsOperationTombstone) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    ASSERT_TRUE(manager.ReservePut(allocation.view.vsegment_id, "put-1", 64));
    ASSERT_EQ(manager.AbortPut(allocation.view.vsegment_id, "put-1"),
              ErrorCode::OK);
    auto stats = manager.Stats();
    EXPECT_EQ(stats.active, 1);
    EXPECT_EQ(stats.reservations, 0);
    EXPECT_EQ(stats.committed_allocations, 0);
    EXPECT_EQ(manager.ForgetOperation("put-1"), ErrorCode::OK);
    EXPECT_EQ(manager.ForgetOperation("put-1"), ErrorCode::OBJECT_NOT_FOUND);
}

TEST(VSegmentManagerTest, RetriesIncompleteInitialPrecreation) {
    auto committer = Committer();
    committer->fail_mutation = "vsegment_create_commit";
    VSegmentManager manager(Config(), {Profile()}, committer);
    EXPECT_EQ(manager.EnsureInitialVSegments(), ErrorCode::PERSISTENT_FAIL);
    EXPECT_EQ(manager.Stats().preparing, 1u);

    EXPECT_EQ(manager.EnsureInitialVSegments(), ErrorCode::OK);
    EXPECT_EQ(manager.Stats().preparing, 0u);
    EXPECT_EQ(manager.Stats().active, 1u);
}

TEST(VSegmentManagerTest, BoundsAbortedOperationTombstones) {
    VSegmentManager manager(Config(), {Profile()}, Committer(), 2);
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    for (const std::string operation : {"z-oldest", "a-middle", "b-newest"}) {
        ASSERT_TRUE(manager.ReservePut(allocation.view.vsegment_id, operation,
                                       8));
        ASSERT_EQ(manager.AbortPut(allocation.view.vsegment_id, operation),
                  ErrorCode::OK);
    }
    const auto state = manager.Snapshot();
    ASSERT_EQ(state.vsegments.size(), 1u);
    EXPECT_LE(state.vsegments[0].logical_allocation.completed_operations.size(),
              2u);
    EXPECT_LE(state.operation_vsegments.size(), 2u);
    EXPECT_FALSE(state.operation_vsegments.contains("z-oldest"));
    EXPECT_TRUE(state.operation_vsegments.contains("a-middle"));
    EXPECT_TRUE(state.operation_vsegments.contains("b-newest"));
}

TEST(VSegmentManagerTest, RecoveryReconcileUsesObjectMetadataAsAuthority) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    ASSERT_TRUE(
        manager.ReservePut(allocation.view.vsegment_id, "committed-op", 64));
    LogicalRange committed_range;
    ASSERT_EQ(manager.CommitPut(allocation.view.vsegment_id, "committed-op",
                                "orphan-object", &committed_range),
              ErrorCode::OK);
    auto pending =
        manager.ReservePut(allocation.view.vsegment_id, "pending-op", 32);
    ASSERT_TRUE(pending);

    VSegmentDescriptor pending_descriptor{
        "partition-1", allocation.view.vsegment_id, pending.range.offset,
        pending.range.length, "pending-op"};
    ASSERT_EQ(manager.ReconcileObjectReferences(
                  {{pending_descriptor, "pending-object", false}}),
              ErrorCode::OK);
    EXPECT_EQ(manager.Stats().committed_allocations, 0);
    EXPECT_EQ(manager.Stats().reservations, 1);

    ASSERT_EQ(manager.AbortPut(allocation.view.vsegment_id, "pending-op"),
              ErrorCode::OK);
    auto full = manager.ReservePut(allocation.view.vsegment_id, "full-op",
                                   allocation.view.logical_capacity);
    EXPECT_TRUE(full) << full.detail;
}

TEST(VSegmentManagerTest, RecoveryReconcileRestoresCommittedReleaseIdentity) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    VSegmentDescriptor descriptor{"partition-1", allocation.view.vsegment_id,
                                  128, 64, "committed-op"};
    ASSERT_EQ(manager.ReconcileObjectReferences(
                  {{descriptor, "tenant/object", true}}),
              ErrorCode::OK);
    EXPECT_EQ(manager.ReleaseObject(allocation.view.vsegment_id,
                                    "tenant/object", {128, 64}),
              ErrorCode::OK);
}

TEST(VSegmentManagerTest, RetirementPersistenceFailureRollsBackExtent) {
    auto committer = Committer();
    VSegmentManager manager(Config(), {Profile()}, committer);
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    ASSERT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                         Lifecycle::DRAINING),
              ErrorCode::OK);

    committer->fail_next = true;
    EXPECT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::RETIRED),
              ErrorCode::PERSISTENT_FAIL);
    VSegmentView view;
    EXPECT_TRUE(manager.FindView(allocation.view.vsegment_id, &view));
    EXPECT_EQ(manager.Stats().draining, 1);

    EXPECT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::RETIRED),
              ErrorCode::OK);
    EXPECT_FALSE(manager.FindView(allocation.view.vsegment_id, &view));
}

TEST(VSegmentHaTest, ReplaysContinuousPartitionRevisions) {
    PartitionVSegmentSnapshot base;
    base.partition_id = "partition-1";
    base.config_generation = 1;
    base.route_epoch = 4;
    base.metadata_revision = 10;
    auto next = base;
    next.metadata_revision = 11;
    VSegmentOpLogRecord record{"partition-1", 4, 11, "logical_reserve",
                               next};
    OpLogEntry entry;
    entry.op_type = OpType::VSEGMENT_STATE;
    struct_json::to_json(record, entry.payload);
    entry.checksum = ComputeOpLogChecksum(entry.payload);

    PartitionVSegmentSnapshot recovered;
    std::string detail;
    EXPECT_EQ(ReplayVSegmentState(base, {entry}, &recovered, &detail),
              ErrorCode::OK)
        << detail;
    EXPECT_EQ(recovered.metadata_revision, 11);
    EXPECT_EQ(recovered.route_epoch, 4);
}

TEST(VSegmentHaTest, RejectsRevisionGapAndStaleEpoch) {
    PartitionVSegmentSnapshot base;
    base.partition_id = "partition-1";
    base.config_generation = 1;
    base.route_epoch = 4;
    base.metadata_revision = 10;
    auto next = base;
    next.metadata_revision = 12;
    VSegmentOpLogRecord record{"partition-1", 4, 12, "logical_reserve",
                               next};
    OpLogEntry entry;
    entry.op_type = OpType::VSEGMENT_STATE;
    struct_json::to_json(record, entry.payload);
    entry.checksum = ComputeOpLogChecksum(entry.payload);
    PartitionVSegmentSnapshot recovered;
    EXPECT_EQ(ReplayVSegmentState(base, {entry}, &recovered),
              ErrorCode::OPLOG_ENTRY_NOT_FOUND);

    record.metadata_revision = 11;
    record.route_epoch = 3;
    record.state.metadata_revision = 11;
    record.state.route_epoch = 3;
    entry.payload.clear();
    struct_json::to_json(record, entry.payload);
    entry.checksum = ComputeOpLogChecksum(entry.payload);
    EXPECT_EQ(ReplayVSegmentState(base, {entry}, &recovered),
              ErrorCode::STALE_ROUTE);
}

TEST(VSegmentHaTest, ReplaysIncrementalDeltaAndLegacyFullState) {
    PartitionVSegmentSnapshot base;
    base.partition_id = "partition-1";
    base.config_generation = 1;
    base.route_epoch = 4;
    base.metadata_revision = 10;
    base.operation_vsegments["old-op"] = "vs-old";

    VSegmentStateSnapshot added{"default", Lifecycle::ACTIVE, View(), {}};
    added.view.partition_id = base.partition_id;
    added.view.vsegment_id = "vs-new";
    added.logical_allocation.logical_capacity = added.view.logical_capacity;
    added.logical_allocation.free_ranges = {
        {0, added.view.logical_capacity}};
    VSegmentOpLogRecord delta;
    delta.partition_id = base.partition_id;
    delta.route_epoch = 4;
    delta.metadata_revision = 11;
    delta.mutation = "logical_reserve";
    delta.full_state = false;
    delta.delta.upserted_vsegments.push_back(added);
    delta.delta.removed_operation_ids.push_back("old-op");
    delta.delta.upserted_operations["new-op"] = "vs-new";
    OpLogEntry delta_entry;
    delta_entry.op_type = OpType::VSEGMENT_STATE;
    struct_json::to_json(delta, delta_entry.payload);
    delta_entry.checksum = ComputeOpLogChecksum(delta_entry.payload);

    PartitionVSegmentSnapshot recovered;
    std::string detail;
    ASSERT_EQ(ReplayVSegmentState(base, {delta_entry}, &recovered, &detail),
              ErrorCode::OK)
        << detail;
    ASSERT_EQ(recovered.vsegments.size(), 1u);
    EXPECT_EQ(recovered.vsegments.front().view.vsegment_id, "vs-new");
    EXPECT_FALSE(recovered.operation_vsegments.count("old-op"));
    EXPECT_EQ(recovered.operation_vsegments.at("new-op"), "vs-new");

    auto legacy_state = recovered;
    legacy_state.metadata_revision = 12;
    LegacyVSegmentOpLogRecord legacy{base.partition_id, 4, 12,
                                     "logical_commit", legacy_state};
    OpLogEntry legacy_entry;
    legacy_entry.op_type = OpType::VSEGMENT_STATE;
    struct_json::to_json(legacy, legacy_entry.payload);
    legacy_entry.checksum = ComputeOpLogChecksum(legacy_entry.payload);
    ASSERT_EQ(ReplayVSegmentState(recovered, {legacy_entry}, &recovered,
                                  &detail),
              ErrorCode::OK)
        << detail;
    EXPECT_EQ(recovered.metadata_revision, 12u);
}

TEST(VSegmentMetricsTest, ExportsCapacityAndFailureSignals) {
    auto& metrics = VSegmentMetrics::Instance();
    metrics.SetCapacity(1024, 256, 2, 3, 1);
    metrics.IncCreateFailure();
    metrics.IncPersistenceFailure();
    metrics.IncStaleRoute();
    const auto text = metrics.Serialize();
    EXPECT_NE(text.find("mooncake_vsegment_logical_capacity_bytes 1024"),
              std::string::npos);
    EXPECT_NE(text.find("mooncake_vsegment_logical_free_bytes 256"),
              std::string::npos);
    EXPECT_NE(text.find("mooncake_vsegment_create_failure_total"),
              std::string::npos);
    EXPECT_NE(text.find("mooncake_vsegment_persistence_failure_total"),
              std::string::npos);
    EXPECT_NE(text.find("mooncake_vsegment_stale_route_total"),
              std::string::npos);
}

TEST(VSegmentHaTest, AcceptedCommitWaitsForDurabilityWithoutRollbackSignal) {
    std::atomic<bool> allow_write{false};
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{},
        [&](const OpLogBatchRecord&, const DurablePrefix&) {
            while (!allow_write.load()) std::this_thread::yield();
            return ErrorCode::OK;
        });
    writer.Start();
    OrderedOpLogVSegmentCommitter committer(
        &writer, std::chrono::milliseconds(5));
    PartitionVSegmentSnapshot state;
    state.partition_id = "partition-1";
    state.config_generation = 1;
    state.route_epoch = 1;
    state.metadata_revision = 1;

    auto result = std::async(std::launch::async, [&] {
        return committer.Commit(state, "logical_reserve", nullptr);
    });
    EXPECT_EQ(result.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);
    allow_write = true;
    EXPECT_EQ(result.get(), ErrorCode::OK);
    writer.Stop();
}

TEST(VSegmentHaTest, DurableMetadataCallbackCanPersistReleaseWithoutDeadlock) {
    OrderedOpLogWriter writer(OrderedOpLogWriterConfig{},
                              [](const OpLogBatchRecord&,
                                 const DurablePrefix&) {
                                  return ErrorCode::OK;
                              });
    writer.Start();
    OrderedOpLogVSegmentCommitter committer(&writer);
    PartitionVSegmentSnapshot state;
    state.partition_id = "partition-1";
    state.config_generation = 1;
    state.route_epoch = 1;
    state.metadata_revision = 1;
    std::promise<ErrorCode> completed;
    auto completion = completed.get_future();

    auto reservation = writer.Reserve();
    ASSERT_TRUE(reservation.has_value());
    OpLogEntry metadata_remove;
    metadata_remove.op_type = OpType::REMOVE;
    metadata_remove.object_key = "object-1";
    metadata_remove.payload = "remove";
    metadata_remove.checksum = ComputeOpLogChecksum(metadata_remove.payload);
    auto pending = writer.Commit(
        std::move(*reservation), std::move(metadata_remove),
        [&](const OpLogEntry&) {
            completed.set_value(
                committer.Commit(state, "logical_release", nullptr));
        });
    ASSERT_TRUE(pending.has_value());
    ASSERT_EQ(completion.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_EQ(completion.get(), ErrorCode::OK);
    writer.Stop();
}

TEST(VSegmentServiceTest, OwnsPartitionPutAndViewLifecycle) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    VSegmentService service(snapshot);
    ASSERT_EQ(service.AddPartition("partition-1", 8, Committer()),
              ErrorCode::OK);

    VSegmentPutStartResult started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = service.StartPut("partition-1", 8, "put-1", 80);
        if (started.error != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started) << started.detail;
    EXPECT_EQ(service.CommitPut(started.replica, 8, "put-1", "object-1"),
              ErrorCode::OK);
    VSegmentView view;
    EXPECT_EQ(service.LoadView("partition-1", started.replica.vsegment_id,
                               &view),
              ErrorCode::OK);
    EXPECT_EQ(view.partition_id, "partition-1");
    EXPECT_EQ(service.StartPut("partition-1", 7, "put-2", 16).error,
              ErrorCode::STALE_ROUTE);
    EXPECT_EQ(service.RemovePartition("partition-1", 9), ErrorCode::OK);
    EXPECT_EQ(service.LoadView("partition-1", view.vsegment_id, &view),
              ErrorCode::STALE_ROUTE);
}

TEST(VSegmentServiceTest, RejectsDescriptorMismatchBeforeCommit) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    VSegmentService service(snapshot);
    ASSERT_EQ(service.AddPartition("partition-1", 8, Committer()),
              ErrorCode::OK);

    VSegmentPutStartResult started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = service.StartPut("partition-1", 8, "put-1", 80);
        if (started.error != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started) << started.detail;
    auto mismatched = started.replica;
    ++mismatched.logical_offset;
    EXPECT_EQ(service.CommitPut(mismatched, 8, "put-1", "object-1"),
              ErrorCode::INVALID_VERSION);
    EXPECT_EQ(service.CommitPut(started.replica, 8, "put-1", "object-1"),
              ErrorCode::OK);
}

TEST(VSegmentServiceTest, RejectsPartitionWithoutPublishedQuota) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    VSegmentService service(snapshot);
    std::string detail;
    EXPECT_EQ(service.AddPartition("partition-2", 1, Committer(), nullptr,
                                   &detail),
              ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT);
}

TEST(VSegmentServiceTest, PrecreatesConfiguredInitialVSegments) {
    auto profile = Profile();
    profile.initial_vsegment_count = 2;
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {profile};
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    VSegmentService service(snapshot);
    ASSERT_EQ(service.AddPartition("partition-1", 1, Committer()),
              ErrorCode::OK);

    PartitionVSegmentSnapshot state;
    ASSERT_EQ(service.SnapshotPartition("partition-1", &state),
              ErrorCode::OK);
    ASSERT_EQ(state.vsegments.size(), 2u);
    EXPECT_TRUE(std::all_of(state.vsegments.begin(), state.vsegments.end(),
                            [](const auto& item) {
                                return item.lifecycle == Lifecycle::ACTIVE;
                            }));
}

TEST(VSegmentServiceTest, ReconcilesManagerFromPartitionRouteEpoch) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    VSegmentService service(snapshot);
    partition::PartitionRoute route;
    route.partition_id.partition_id = "partition-1";
    route.owner_submaster_id = "submaster-a";
    route.route_epoch = 3;
    route.state = static_cast<int32_t>(partition::PartitionState::kActive);

    EXPECT_EQ(service.ReconcilePartitionRoute(route, "submaster-a",
                                              Committer()),
              ErrorCode::OK);
    route.state =
        static_cast<int32_t>(partition::PartitionState::kMigrating);
    route.target_submaster_id = "submaster-b";
    route.route_epoch = 4;
    EXPECT_EQ(service.ReconcilePartitionRoute(route, "submaster-a",
                                              Committer()),
              ErrorCode::OK);
    EXPECT_EQ(service.StartPut("partition-1", 3, "stale", 16).error,
              ErrorCode::STALE_ROUTE);

    route.owner_submaster_id = "submaster-b";
    route.target_submaster_id.clear();
    route.state = static_cast<int32_t>(partition::PartitionState::kActive);
    route.route_epoch = 5;
    EXPECT_EQ(service.ReconcilePartitionRoute(route, "submaster-a", nullptr),
              ErrorCode::OK);
    EXPECT_EQ(service.StartPut("partition-1", 5, "not-owner", 16).error,
              ErrorCode::STALE_ROUTE);
}

TEST(VSegmentServiceTest, ReleasesCommittedObjectRange) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    VSegmentService service(snapshot);
    ASSERT_EQ(service.AddPartition("partition-1", 1, Committer()),
              ErrorCode::OK);
    VSegmentPutStartResult started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = service.StartPut("partition-1", 1, "put-release", 64);
        if (started.error != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started) << started.detail;
    ASSERT_EQ(service.CommitPut(started.replica, 1, "put-release", "object-1"),
              ErrorCode::OK);
    EXPECT_EQ(service.ReleaseObject(started.replica, "object-1"),
              ErrorCode::OK);
    EXPECT_EQ(service.ReleaseObject(started.replica, "object-1"),
              ErrorCode::OK);
}

TEST(VSegmentServiceTest, AbortReturnsReservedLogicalRange) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 256}, {"segment-b", 0, 256}}}};
    VSegmentService service(snapshot);
    ASSERT_EQ(service.AddPartition("partition-1", 1, Committer()),
              ErrorCode::OK);

    VSegmentPutStartResult started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = service.StartPut("partition-1", 1, "put-abort", 64);
        if (started.error != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started) << started.detail;
    ASSERT_EQ(service.AbortPut("partition-1", started.replica.vsegment_id, 1,
                               "put-abort"),
              ErrorCode::OK);

    PartitionVSegmentSnapshot state;
    ASSERT_EQ(service.SnapshotPartition("partition-1", &state),
              ErrorCode::OK);
    ASSERT_EQ(state.vsegments.size(), 1u);
    EXPECT_TRUE(state.vsegments[0].logical_allocation.reservations.empty());
    ASSERT_EQ(state.vsegments[0].logical_allocation.free_ranges.size(), 1u);
    EXPECT_EQ(state.vsegments[0].logical_allocation.free_ranges[0].offset,
              0u);
    EXPECT_EQ(state.vsegments[0].logical_allocation.free_ranges[0].length,
              state.vsegments[0].view.logical_capacity);
}

TEST(VSegmentServiceTest, ReplicaAllocationFailureAbortsEarlierReservations) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    // Exactly one vsegment can be created, while the request needs two
    // distinct vsegments.
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 256}, {"segment-b", 0, 256}}}};
    VSegmentService service(snapshot);
    ASSERT_EQ(service.AddPartition("partition-1", 1, Committer()),
              ErrorCode::OK);

    tl::expected<std::vector<VSegmentPutStartResult>, ErrorCode> result;
    for (int attempt = 0; attempt < 40; ++attempt) {
        const auto suffix = std::to_string(attempt);
        result = service.StartPutReplicasOwned(
            "partition-1",
            {"replica-1-" + suffix, "replica-2-" + suffix}, 64);
        if (result || result.error() != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(),
              ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT);

    PartitionVSegmentSnapshot state;
    ASSERT_EQ(service.SnapshotPartition("partition-1", &state),
              ErrorCode::OK);
    ASSERT_EQ(state.vsegments.size(), 1u);
    EXPECT_TRUE(state.vsegments[0].logical_allocation.reservations.empty());
}

TEST(VSegmentServiceTest, SurfacesReplicaRollbackPersistenceFailure) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 256}, {"segment-b", 0, 256}}}};
    auto committer = Committer();
    VSegmentService service(snapshot);
    ASSERT_EQ(service.AddPartition("partition-1", 1, committer),
              ErrorCode::OK);
    committer->fail_mutation = "logical_abort";

    auto result = service.StartPutReplicasOwned(
        "partition-1", {"rollback-1", "rollback-2"}, 64);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::PERSISTENT_FAIL);

    PartitionVSegmentSnapshot state;
    ASSERT_EQ(service.SnapshotPartition("partition-1", &state),
              ErrorCode::OK);
    ASSERT_EQ(state.vsegments.size(), 1u);
    EXPECT_EQ(state.vsegments[0].logical_allocation.reservations.size(), 1u);
}

TEST(VSegmentServiceTest, RestoresMigratedPartitionAtNewRouteEpoch) {
    PartitionPhysicalQuotaSnapshot quota;
    quota.config_generation = 1;
    quota.policy_digest = "policy";
    quota.default_profile = "default";
    quota.profile_specs = {Profile()};
    quota.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    VSegmentService source(quota);
    ASSERT_EQ(source.AddPartition("partition-1", 3, Committer()),
              ErrorCode::OK);

    VSegmentPutStartResult started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = source.StartPut("partition-1", 3, "put-before-move", 64);
        if (started.error != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started) << started.detail;
    ASSERT_EQ(source.CommitPut(started.replica, 3, "put-before-move",
                               "object-1"),
              ErrorCode::OK);
    PartitionVSegmentSnapshot recovered;
    ASSERT_EQ(source.SnapshotPartition("partition-1", &recovered),
              ErrorCode::OK);

    VSegmentService target(quota);
    ASSERT_EQ(target.AddPartition("partition-1", 4, Committer(), &recovered),
              ErrorCode::OK);
    VSegmentView restored_view;
    EXPECT_EQ(target.LoadView("partition-1", started.replica.vsegment_id,
                              &restored_view),
              ErrorCode::OK);
    EXPECT_EQ(target.StartPut("partition-1", 3, "stale", 16).error,
              ErrorCode::STALE_ROUTE);
    auto next = target.StartPut("partition-1", 4, "put-after-move", 16);
    ASSERT_TRUE(next) << next.detail;
    EXPECT_EQ(next.replica.vsegment_id, started.replica.vsegment_id);
    EXPECT_EQ(next.replica.logical_offset,
              started.replica.logical_offset + started.replica.length);
}

TEST(VSegmentIntegrationTest, OrdinaryPutReturnsAndCommitsVSegmentReplica) {
    const std::string key = "ordinary-vsegment-put";
    const auto& tenant = TenantId::Default();
    const std::string partition_id =
        std::to_string(cvm::KeySlot(tenant, key));
    PartitionPhysicalQuotaSnapshot quota;
    quota.config_generation = 1;
    quota.policy_digest = "policy";
    quota.default_profile = "default";
    quota.profile_specs = {Profile()};
    quota.quotas = {
        {partition_id, "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    auto vsegments = std::make_shared<VSegmentService>(quota);
    ASSERT_EQ(vsegments->AddPartition(partition_id, 1, Committer()),
              ErrorCode::OK);

    MasterServiceConfig config;
    config.default_kv_lease_ttl = 10000;
    MasterService master(config);
    master.SetVSegmentService(vsegments);
    ReplicateConfig replicas;
    replicas.replica_num = 1;
    UUID client = generate_uuid();
    tl::expected<std::optional<PutStartResult>, ErrorCode> started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = master.TryVSegmentPutStart(client, key, tenant, 64,
                                             replicas);
        if (started && started->has_value()) break;
        if (!started && started.error() != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started.has_value());
    ASSERT_TRUE(started->has_value());
    ASSERT_EQ((**started).replicas.size(), 1u);
    EXPECT_TRUE((**started).replicas.front().is_vsegment_replica());

    ObjectMeta object{key, std::nullopt};
    // The ordinary client API still describes this as its memory write path;
    // the server maps that completion to the returned VSEGMENT replica.
    ASSERT_TRUE(master.PutEnd(client, object, tenant, ReplicaType::MEMORY,
                              (**started).operation_id));
    auto loaded = master.GetReplicaList(key, tenant);
    ASSERT_TRUE(loaded.has_value());
    ASSERT_EQ(loaded->replicas.size(), 1u);
    EXPECT_TRUE(loaded->replicas.front().is_vsegment_replica());
}

TEST(VSegmentIntegrationTest, OrdinaryMemoryRevokeRemovesVSegmentReplica) {
    const std::string key = "ordinary-vsegment-revoke";
    const auto& tenant = TenantId::Default();
    const std::string partition_id =
        std::to_string(cvm::KeySlot(tenant, key));
    PartitionPhysicalQuotaSnapshot quota;
    quota.config_generation = 1;
    quota.policy_digest = "policy";
    quota.default_profile = "default";
    quota.profile_specs = {Profile()};
    quota.quotas = {
        {partition_id, "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    auto vsegments = std::make_shared<VSegmentService>(quota);
    ASSERT_EQ(vsegments->AddPartition(partition_id, 1, Committer()),
              ErrorCode::OK);

    MasterService master;
    master.SetVSegmentService(vsegments);
    ReplicateConfig config;
    UUID client = generate_uuid();
    tl::expected<std::optional<PutStartResult>, ErrorCode> started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = master.TryVSegmentPutStart(client, key, tenant, 64, config);
        if (started && started->has_value()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started && started->has_value());
    ASSERT_TRUE(master.PutRevoke(client, key, tenant, ReplicaType::MEMORY,
                                 (**started).operation_id));
    EXPECT_EQ(master.GetReplicaList(key, tenant).error(),
              ErrorCode::OBJECT_NOT_FOUND);
}

TEST(VSegmentIntegrationTest, SizeChangingUpsertReleasesOldAndNewRanges) {
    const std::string key = "vsegment-upsert-release";
    const auto& tenant = TenantId::Default();
    const std::string partition_id =
        std::to_string(cvm::KeySlot(tenant, key));
    PartitionPhysicalQuotaSnapshot quota;
    quota.config_generation = 1;
    quota.policy_digest = "policy";
    quota.default_profile = "default";
    quota.profile_specs = {Profile()};
    quota.quotas = {
        {partition_id, "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    auto vsegments = std::make_shared<VSegmentService>(quota);
    ASSERT_EQ(vsegments->AddPartition(partition_id, 1, Committer()),
              ErrorCode::OK);
    MasterService master;
    master.SetVSegmentService(vsegments);
    ReplicateConfig config;
    UUID client = generate_uuid();

    tl::expected<std::optional<PutStartResult>, ErrorCode> started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = master.TryVSegmentPutStart(client, key, tenant, 64, config);
        if (started && started->has_value()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started && started->has_value());
    const auto original = (**started).replicas.front()
                              .get_vsegment_descriptor()
                              .vsegment_id;
    ASSERT_TRUE(master.PutEnd(client, key, tenant, ReplicaType::MEMORY,
                              (**started).operation_id));

    auto upsert = master.UpsertStart(client, key, tenant, 128, config);
    ASSERT_TRUE(upsert.has_value());
    ASSERT_TRUE(master.PutRevoke(client, key, tenant, ReplicaType::MEMORY));

    auto full = vsegments->StartPutOwned(partition_id, "full-after-upsert",
                                         Profile().member_extent_size * 2);
    ASSERT_TRUE(full) << full.detail;
    EXPECT_EQ(full.replica.vsegment_id, original);
}

TEST(VSegmentServiceTest, ObjectReplicasUseDistinctVSegments) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    VSegmentService service(snapshot);
    ASSERT_EQ(service.AddPartition("partition-1", 1, Committer()),
              ErrorCode::OK);
    tl::expected<std::vector<VSegmentPutStartResult>, ErrorCode> result;
    for (int attempt = 0; attempt < 40; ++attempt) {
        result = service.StartPutReplicasOwned(
            "partition-1", {"replica-op-1", "replica-op-2"}, 64);
        if (result || result.error() != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(result.has_value()) << toString(result.error());
    ASSERT_EQ(result->size(), 2u);
    EXPECT_NE((*result)[0].replica.vsegment_id,
              (*result)[1].replica.vsegment_id);
}

TEST(VSegmentServiceTest, OrdinaryPutSelectsConfiguredProfile) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "profile-selected-object";
    const std::string partition_id =
        std::to_string(cvm::KeySlot(tenant, key));
    auto alternate = Profile();
    alternate.name = "alternate";
    PartitionPhysicalQuotaSnapshot quota;
    quota.config_generation = 1;
    quota.policy_digest = "policy";
    quota.default_profile = "default";
    quota.profile_specs = {Profile(), alternate};
    quota.quotas = {
        {partition_id, "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}},
        {partition_id, "alternate", "DRAM",
         {{"segment-a", 512, 512}, {"segment-b", 512, 512}}}};
    auto vsegments = std::make_shared<VSegmentService>(quota);
    ASSERT_EQ(vsegments->AddPartition(partition_id, 1, Committer()),
              ErrorCode::OK);
    MasterService master;
    master.SetVSegmentService(vsegments);
    ReplicateConfig config;
    config.vsegment_profile_name = "alternate";
    const UUID client = generate_uuid();

    tl::expected<std::optional<PutStartResult>, ErrorCode> started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = master.TryVSegmentPutStart(client, key, tenant, 64, config);
        if (started && started->has_value()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started && started->has_value());
    const auto states = vsegments->SnapshotAllPartitions();
    ASSERT_EQ(states.size(), 1u);
    ASSERT_EQ(states.front().vsegments.size(), 2u);
    EXPECT_EQ(std::count_if(states.front().vsegments.begin(),
                            states.front().vsegments.end(),
                            [](const auto& state) {
                                return state.profile_name == "alternate";
                            }),
              1);
}

// ---- 自动发现：BuildDiscoveredQuotaPlan ----

TEST(BuildDiscoveredQuotaPlanTest, AcceptsNonExclusiveSegmentsByDefault) {
    // 非独占 segment 默认接受：按 [used_bytes, capacity) 切分。
    // used_bytes=0（空集群）时等同独占，base_offset=0。
    VSegmentUserPolicy policy;
    policy.member_count = 2;
    policy.stripe_size = 64;
    policy.member_extent_size = 256;
    policy.required_medium = "DRAM";

    cvm::SegmentDescriptor seg_a;
    seg_a.segment_id = "seg-a";
    seg_a.capacity = 4096;
    seg_a.te_endpoint = "host-a:1234";
    seg_a.host_id = "host-a";
    seg_a.medium = "DRAM";
    seg_a.io_alignment = 8;
    seg_a.vsegment_exclusive = false;  // 非独占，默认接受
    seg_a.used_bytes = 0;              // 空集群
    cvm::SegmentDescriptor seg_b = seg_a;
    seg_b.segment_id = "seg-b";

    std::vector<cvm::SegmentDescriptor> descriptors = {seg_a, seg_b};
    cvm::MasterRegistration master;
    master.master_id = "master-a";
    std::vector<cvm::MasterRegistration> masters = {master};
    cvm::MountEntry mount_a, mount_b;
    mount_a.segment_id = "seg-a";
    mount_b.segment_id = "seg-b";
    std::vector<std::pair<std::string, cvm::MountEntry>> mounts = {
        {"master-a", mount_a}, {"master-a", mount_b}};

    PartitionQuotaPlanRequest request;
    std::string detail;
    auto error = BuildDiscoveredQuotaPlan(policy, descriptors, masters, mounts,
                                          &request, &detail);
    ASSERT_EQ(error, ErrorCode::OK) << detail;
    ASSERT_EQ(request.segments.size(), 2u);
    // used_bytes=0 → base_offset=0，capacity=全部可用。
    EXPECT_EQ(request.segments[0].base_offset, 0u);
    EXPECT_EQ(request.segments[0].capacity, 4096u);
}

TEST(BuildDiscoveredQuotaPlanTest, AcceptsExclusiveSegmentsAndReadsRealFacts) {
    VSegmentUserPolicy policy;
    policy.member_count = 2;
    policy.stripe_size = 64;
    policy.member_extent_size = 256;
    policy.required_medium = "DRAM";

    cvm::SegmentDescriptor seg_a;
    seg_a.segment_id = "seg-a";
    seg_a.capacity = 4096;
    seg_a.te_endpoint = "host-a:1234";
    seg_a.host_id = "host-a";
    seg_a.medium = "DRAM";
    seg_a.io_alignment = 8;
    seg_a.supports_unaligned_io = true;
    seg_a.failure_domain = "rack-1";
    seg_a.vsegment_exclusive = true;
    seg_a.used_bytes = 0;  // 独占必须 used_bytes=0
    cvm::SegmentDescriptor seg_b = seg_a;
    seg_b.segment_id = "seg-b";
    seg_b.host_id = "host-b";
    seg_b.failure_domain.clear();  // 验证 host_id 兜底

    std::vector<cvm::SegmentDescriptor> descriptors = {seg_a, seg_b};
    cvm::MasterRegistration master;
    master.master_id = "master-a";
    std::vector<cvm::MasterRegistration> masters = {master};
    cvm::MountEntry mount_a, mount_b;
    mount_a.segment_id = "seg-a";
    mount_b.segment_id = "seg-b";
    std::vector<std::pair<std::string, cvm::MountEntry>> mounts = {
        {"master-a", mount_a}, {"master-a", mount_b}};

    PartitionQuotaPlanRequest request;
    std::string detail;
    auto error = BuildDiscoveredQuotaPlan(policy, descriptors, masters, mounts,
                                          &request, &detail);
    ASSERT_EQ(error, ErrorCode::OK) << detail;
    ASSERT_EQ(request.segments.size(), 2u);
    // 资源事实从 SegmentDescriptor 读取，不再硬编码。
    EXPECT_EQ(request.segments[0].medium, "DRAM");
    EXPECT_EQ(request.segments[0].io_alignment, 8u);
    EXPECT_EQ(request.segments[0].failure_domain, "rack-1");
    EXPECT_EQ(request.segments[1].failure_domain, "host-b");  // host_id 兜底
    EXPECT_TRUE(request.segments[0].supports_unaligned_io);
    // 独占 + used_bytes=0 → base_offset=0，capacity=全部可用。
    EXPECT_EQ(request.segments[0].base_offset, 0u);
    EXPECT_EQ(request.segments[0].capacity, 4096u);
    // Partition 列表由 KV PT 生成（slot 数）。
    EXPECT_EQ(request.partition_ids.size(), cvm::kSlotCount);
    EXPECT_EQ(request.profile_specs.size(), 1u);
    EXPECT_EQ(request.profile_specs[0].member_count, 2u);
}

TEST(BuildDiscoveredQuotaPlanTest, RejectsEmptyMediumFromAllocator) {
    VSegmentUserPolicy policy;
    policy.member_count = 1;
    policy.stripe_size = 64;
    policy.member_extent_size = 256;

    cvm::SegmentDescriptor seg;
    seg.segment_id = "seg-a";
    seg.capacity = 4096;
    seg.te_endpoint = "host-a:1234";
    seg.medium = "";  // allocator 未上报 → 拒绝
    seg.vsegment_exclusive = true;

    std::vector<cvm::SegmentDescriptor> descriptors = {seg};
    cvm::MasterRegistration master;
    master.master_id = "master-a";
    std::vector<cvm::MasterRegistration> masters = {master};
    cvm::MountEntry mount;
    mount.segment_id = "seg-a";
    std::vector<std::pair<std::string, cvm::MountEntry>> mounts = {
        {"master-a", mount}};

    PartitionQuotaPlanRequest request;
    std::string detail;
    auto error = BuildDiscoveredQuotaPlan(policy, descriptors, masters, mounts,
                                          &request, &detail);
    EXPECT_EQ(error, ErrorCode::INVALID_PARAMS);
    EXPECT_NE(detail.find("empty medium"), std::string::npos);
}

TEST(BuildDiscoveredQuotaPlanTest,
     AcceptsNonExclusiveWithUsedBytesAndSlicesCorrectly) {
    // 非独占 + used_bytes>0 → 按 [used_bytes, capacity) 切分，
    // base_offset=used_bytes，capacity=available。
    VSegmentUserPolicy policy;
    policy.member_count = 2;
    policy.stripe_size = 64;
    policy.member_extent_size = 256;
    policy.required_medium = "DRAM";

    cvm::SegmentDescriptor seg_a;
    seg_a.segment_id = "seg-a";
    seg_a.capacity = 8192;
    seg_a.te_endpoint = "host-a:1234";
    seg_a.host_id = "host-a";
    seg_a.medium = "DRAM";
    seg_a.io_alignment = 8;
    seg_a.vsegment_exclusive = false;
    seg_a.used_bytes = 4096;  // 前半段已被其他分配器占用
    cvm::SegmentDescriptor seg_b = seg_a;
    seg_b.segment_id = "seg-b";

    std::vector<cvm::SegmentDescriptor> descriptors = {seg_a, seg_b};
    cvm::MasterRegistration master;
    master.master_id = "master-a";
    std::vector<cvm::MasterRegistration> masters = {master};
    cvm::MountEntry mount_a, mount_b;
    mount_a.segment_id = "seg-a";
    mount_b.segment_id = "seg-b";
    std::vector<std::pair<std::string, cvm::MountEntry>> mounts = {
        {"master-a", mount_a}, {"master-a", mount_b}};

    PartitionQuotaPlanRequest request;
    std::string detail;
    auto error = BuildDiscoveredQuotaPlan(policy, descriptors, masters, mounts,
                                          &request, &detail);
    ASSERT_EQ(error, ErrorCode::OK) << detail;
    ASSERT_EQ(request.segments.size(), 2u);
    // base_offset=used_bytes，capacity=available=capacity-used_bytes。
    EXPECT_EQ(request.segments[0].base_offset, 4096u);
    EXPECT_EQ(request.segments[0].capacity, 4096u);  // 8192 - 4096
}

TEST(BuildDiscoveredQuotaPlanTest, RejectsExclusiveWithNonZeroUsedBytes) {
    // exclusive=true 但 used_bytes>0 → 声明与实际不符，拒绝。
    VSegmentUserPolicy policy;
    policy.member_count = 1;
    policy.stripe_size = 64;
    policy.member_extent_size = 256;
    policy.required_medium = "DRAM";

    cvm::SegmentDescriptor seg;
    seg.segment_id = "seg-a";
    seg.capacity = 8192;
    seg.te_endpoint = "host-a:1234";
    seg.medium = "DRAM";
    seg.vsegment_exclusive = true;
    seg.used_bytes = 100;  // 声明独占但有占用 → 拒绝

    std::vector<cvm::SegmentDescriptor> descriptors = {seg};
    cvm::MasterRegistration master;
    master.master_id = "master-a";
    std::vector<cvm::MasterRegistration> masters = {master};
    cvm::MountEntry mount;
    mount.segment_id = "seg-a";
    std::vector<std::pair<std::string, cvm::MountEntry>> mounts = {
        {"master-a", mount}};

    PartitionQuotaPlanRequest request;
    std::string detail;
    auto error = BuildDiscoveredQuotaPlan(policy, descriptors, masters, mounts,
                                          &request, &detail);
    EXPECT_EQ(error, ErrorCode::INVALID_PARAMS);
    EXPECT_NE(detail.find("vsegment_exclusive=true but used_bytes"), std::string::npos);
}

TEST(BuildDiscoveredQuotaPlanTest, SkipsSegmentsWithInsufficientFreeSpace) {
    // available < member_extent_size → 跳过该 segment。
    VSegmentUserPolicy policy;
    policy.member_count = 1;
    policy.stripe_size = 64;
    policy.member_extent_size = 512;  // 需要至少 512 字节可用
    policy.required_medium = "DRAM";

    cvm::SegmentDescriptor seg;
    seg.segment_id = "seg-a";
    seg.capacity = 4096;
    seg.te_endpoint = "host-a:1234";
    seg.medium = "DRAM";
    seg.vsegment_exclusive = false;
    seg.used_bytes = 3900;  // available=196 < 512 → 跳过

    std::vector<cvm::SegmentDescriptor> descriptors = {seg};
    cvm::MasterRegistration master;
    master.master_id = "master-a";
    std::vector<cvm::MasterRegistration> masters = {master};
    cvm::MountEntry mount;
    mount.segment_id = "seg-a";
    std::vector<std::pair<std::string, cvm::MountEntry>> mounts = {
        {"master-a", mount}};

    PartitionQuotaPlanRequest request;
    std::string detail;
    auto error = BuildDiscoveredQuotaPlan(policy, descriptors, masters, mounts,
                                          &request, &detail);
    EXPECT_EQ(error, ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT);
    EXPECT_NE(detail.find("insufficient"), std::string::npos);
    EXPECT_NE(detail.find("seg-a"), std::string::npos);
}

TEST(BuildDiscoveredQuotaPlanTest, AllowNonExclusiveIsDeprecatedNoop) {
    // allow_non_exclusive 现在是 no-op：非独占 segment 默认接受，
    // 传 true/false 行为一致。
    VSegmentUserPolicy policy;
    policy.member_count = 2;
    policy.stripe_size = 64;
    policy.member_extent_size = 256;
    policy.required_medium = "DRAM";

    cvm::SegmentDescriptor seg_a;
    seg_a.segment_id = "seg-a";
    seg_a.capacity = 4096;
    seg_a.te_endpoint = "host-a:1234";
    seg_a.host_id = "host-a";
    seg_a.medium = "DRAM";
    seg_a.io_alignment = 8;
    seg_a.vsegment_exclusive = false;
    cvm::SegmentDescriptor seg_b = seg_a;
    seg_b.segment_id = "seg-b";

    std::vector<cvm::SegmentDescriptor> descriptors = {seg_a, seg_b};
    cvm::MasterRegistration master;
    master.master_id = "master-a";
    std::vector<cvm::MasterRegistration> masters = {master};
    cvm::MountEntry mount_a, mount_b;
    mount_a.segment_id = "seg-a";
    mount_b.segment_id = "seg-b";
    std::vector<std::pair<std::string, cvm::MountEntry>> mounts = {
        {"master-a", mount_a}, {"master-a", mount_b}};

    // 不传 allow_non_exclusive（默认 false）→ 接受。
    {
        PartitionQuotaPlanRequest request;
        std::string detail;
        auto error = BuildDiscoveredQuotaPlan(policy, descriptors, masters,
                                              mounts, &request, &detail);
        ASSERT_EQ(error, ErrorCode::OK) << detail;
        EXPECT_EQ(request.segments.size(), 2u);
    }
    // 传 allow_non_exclusive=true → 行为一致。
    {
        PartitionQuotaPlanRequest request;
        std::string detail;
        auto error = BuildDiscoveredQuotaPlan(policy, descriptors, masters,
                                              mounts, &request, &detail,
                                              /*allow_non_exclusive=*/true);
        ASSERT_EQ(error, ErrorCode::OK) << detail;
        EXPECT_EQ(request.segments.size(), 2u);
    }
}

// auto 模式：required_medium 为空时，系统自动发现所有介质，为每种介质
// 建独立 profile。混合介质集群（DRAM + NVMe）无需用户手动配置 required_medium。
TEST(BuildDiscoveredQuotaPlanTest, AutoMediumModeBuildsProfilePerMedium) {
    VSegmentUserPolicy policy;
    policy.member_count = 2;
    policy.stripe_size = 64;
    policy.member_extent_size = 256;
    policy.required_medium.clear();  // auto 模式

    cvm::SegmentDescriptor dram_a;
    dram_a.segment_id = "dram-a";
    dram_a.capacity = 4096;
    dram_a.te_endpoint = "host-a:1234";
    dram_a.host_id = "host-a";
    dram_a.medium = "DRAM";
    dram_a.io_alignment = 8;
    dram_a.used_bytes = 0;
    cvm::SegmentDescriptor dram_b = dram_a;
    dram_b.segment_id = "dram-b";
    cvm::SegmentDescriptor nvme_a = dram_a;
    nvme_a.segment_id = "nvme-a";
    nvme_a.medium = "NVMe";
    cvm::SegmentDescriptor nvme_b = nvme_a;
    nvme_b.segment_id = "nvme-b";

    std::vector<cvm::SegmentDescriptor> descriptors = {dram_a, dram_b,
                                                       nvme_a, nvme_b};
    cvm::MasterRegistration master;
    master.master_id = "master-a";
    std::vector<cvm::MasterRegistration> masters = {master};
    std::vector<std::pair<std::string, cvm::MountEntry>> mounts = {
        {"master-a", {{"dram-a"}, {}, {}}},
        {"master-a", {{"dram-b"}, {}, {}}},
        {"master-a", {{"nvme-a"}, {}, {}}},
        {"master-a", {{"nvme-b"}, {}, {}}},
    };

    PartitionQuotaPlanRequest request;
    std::string detail;
    auto error = BuildDiscoveredQuotaPlan(policy, descriptors, masters, mounts,
                                          &request, &detail);
    ASSERT_EQ(error, ErrorCode::OK) << detail;
    // 两种介质各 2 个 segment，都 >= member_count=2，都建 profile。
    ASSERT_EQ(request.segments.size(), 4u);
    ASSERT_EQ(request.profile_specs.size(), 2u);
    // profile 名格式：profile_name-medium
    EXPECT_EQ(request.profile_specs[0].name, "default-DRAM");
    EXPECT_EQ(request.profile_specs[0].required_medium, "DRAM");
    EXPECT_EQ(request.profile_specs[1].name, "default-NVMe");
    EXPECT_EQ(request.profile_specs[1].required_medium, "NVMe");
    // default_profile 选字典序首个介质对应的 profile。
    EXPECT_EQ(request.default_profile, "default-DRAM");
    // 每个 profile 继承用户配的 3 个核心参数。
    for (const auto& profile : request.profile_specs) {
        EXPECT_EQ(profile.member_count, 2u);
        EXPECT_EQ(profile.stripe_size, 64u);
        EXPECT_EQ(profile.member_extent_size, 256u);
    }

    // Plan 应成功，每种介质各建一套 quota。
    PartitionQuotaPlanner planner;
    auto result = planner.Plan(request);
    ASSERT_EQ(result.error, ErrorCode::OK) << result.detail;
    // 每个 partition 有 2 个 quota（DRAM 一个、NVMe 一个）。
    // partition_ids.size() == kSlotCount，quotas.size() == 2 * kSlotCount。
    EXPECT_EQ(result.snapshot.quotas.size(),
              request.partition_ids.size() * 2);
}

// auto 模式：介质 segment 数 < member_count 时跳过该介质。
TEST(BuildDiscoveredQuotaPlanTest, AutoMediumModeSkipsInsufficientMedium) {
    VSegmentUserPolicy policy;
    policy.member_count = 2;
    policy.stripe_size = 64;
    policy.member_extent_size = 256;
    policy.required_medium.clear();  // auto 模式

    cvm::SegmentDescriptor seg;
    seg.capacity = 4096;
    seg.te_endpoint = "host-a:1234";
    seg.host_id = "host-a";
    seg.io_alignment = 8;
    seg.used_bytes = 0;
    // DRAM 只有 1 个（不足 member_count=2），NVMe 有 2 个（足够）。
    cvm::SegmentDescriptor dram_a = seg;
    dram_a.segment_id = "dram-a";
    dram_a.medium = "DRAM";
    cvm::SegmentDescriptor nvme_a = seg;
    nvme_a.segment_id = "nvme-a";
    nvme_a.medium = "NVMe";
    cvm::SegmentDescriptor nvme_b = nvme_a;
    nvme_b.segment_id = "nvme-b";

    std::vector<cvm::SegmentDescriptor> descriptors = {dram_a, nvme_a, nvme_b};
    cvm::MasterRegistration master;
    master.master_id = "master-a";
    std::vector<cvm::MasterRegistration> masters = {master};
    std::vector<std::pair<std::string, cvm::MountEntry>> mounts = {
        {"master-a", {{"dram-a"}, {}, {}}},
        {"master-a", {{"nvme-a"}, {}, {}}},
        {"master-a", {{"nvme-b"}, {}, {}}},
    };

    PartitionQuotaPlanRequest request;
    std::string detail;
    auto error = BuildDiscoveredQuotaPlan(policy, descriptors, masters, mounts,
                                          &request, &detail);
    ASSERT_EQ(error, ErrorCode::OK) << detail;
    // DRAM 跳过，只保留 NVMe 的 2 个 segment。
    ASSERT_EQ(request.segments.size(), 2u);
    ASSERT_EQ(request.profile_specs.size(), 1u);
    EXPECT_EQ(request.profile_specs[0].required_medium, "NVMe");
    EXPECT_EQ(request.default_profile, "default-NVMe");
}

}  // namespace
}  // namespace mooncake::vsegment
