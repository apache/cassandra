/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.net;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.batchlog.Batch;
import org.apache.cassandra.batchlog.BatchRemoveVerbHandler;
import org.apache.cassandra.batchlog.BatchStoreVerbHandler;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.CounterMutationVerbHandler;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationVerbHandler;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadRepairVerbHandler;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SnapshotCommand;
import org.apache.cassandra.db.TruncateResponse;
import org.apache.cassandra.db.TruncateVerbHandler;
import org.apache.cassandra.db.TruncateRequest;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.GossipDigestAck;
import org.apache.cassandra.gms.GossipDigestAck2;
import org.apache.cassandra.gms.GossipDigestAck2VerbHandler;
import org.apache.cassandra.gms.GossipDigestAckVerbHandler;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.gms.GossipDigestSynVerbHandler;
import org.apache.cassandra.gms.GossipShutdownVerbHandler;
import org.apache.cassandra.hints.HintMessage;
import org.apache.cassandra.hints.HintVerbHandler;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.repair.RepairMessageVerbHandler;
import org.apache.cassandra.repair.messages.CleanupMessage;
import org.apache.cassandra.repair.messages.FailSession;
import org.apache.cassandra.repair.messages.FinalizeCommit;
import org.apache.cassandra.repair.messages.FinalizePromise;
import org.apache.cassandra.repair.messages.FinalizePropose;
import org.apache.cassandra.repair.messages.PrepareConsistentRequest;
import org.apache.cassandra.repair.messages.PrepareConsistentResponse;
import org.apache.cassandra.repair.messages.PrepareMessage;
import org.apache.cassandra.repair.messages.SnapshotMessage;
import org.apache.cassandra.repair.messages.StatusRequest;
import org.apache.cassandra.repair.messages.StatusResponse;
import org.apache.cassandra.repair.messages.SyncResponse;
import org.apache.cassandra.repair.messages.SyncRequest;
import org.apache.cassandra.repair.messages.ValidationResponse;
import org.apache.cassandra.repair.messages.ValidationRequest;
import org.apache.cassandra.schema.SchemaMutationsSerializer;
import org.apache.cassandra.schema.SchemaPullVerbHandler;
import org.apache.cassandra.schema.SchemaPushVerbHandler;
import org.apache.cassandra.schema.SchemaVersionVerbHandler;
import org.apache.cassandra.service.accord.serializers.ApplyAndCheckSerializers;
import org.apache.cassandra.service.accord.serializers.BeginInvalidateSerializers;
import org.apache.cassandra.service.accord.serializers.CheckStatusSerializers;
import org.apache.cassandra.service.accord.serializers.InformOfPersistenceSerializers;
import org.apache.cassandra.service.accord.serializers.InformOfTxnSerializers;
import org.apache.cassandra.service.paxos.PaxosCommit;
import org.apache.cassandra.service.paxos.PaxosCommitAndPrepare;
import org.apache.cassandra.service.paxos.PaxosPrepare;
import org.apache.cassandra.service.paxos.PaxosPrepareRefresh;
import org.apache.cassandra.service.paxos.PaxosPropose;
import org.apache.cassandra.service.paxos.PaxosRepair;
import org.apache.cassandra.service.paxos.cleanup.PaxosCleanupHistory;
import org.apache.cassandra.service.paxos.cleanup.PaxosCleanupRequest;
import org.apache.cassandra.service.paxos.cleanup.PaxosCleanupResponse;
import org.apache.cassandra.service.paxos.cleanup.PaxosCleanupComplete;
import org.apache.cassandra.service.paxos.cleanup.PaxosStartPrepareCleanup;
import org.apache.cassandra.service.paxos.cleanup.PaxosFinishPrepareCleanup;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.serializers.AcceptSerializers;
import org.apache.cassandra.service.accord.serializers.ApplySerializers;
import org.apache.cassandra.service.accord.serializers.CommitSerializers;
import org.apache.cassandra.service.accord.serializers.PreacceptSerializers;
import org.apache.cassandra.service.accord.serializers.ReadDataSerializers;
import org.apache.cassandra.service.accord.serializers.RecoverySerializers;
import org.apache.cassandra.service.accord.serializers.WaitOnCommitSerializer;
import org.apache.cassandra.utils.BooleanSerializer;
import org.apache.cassandra.service.EchoVerbHandler;
import org.apache.cassandra.service.SnapshotVerbHandler;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.Commit.Agreed;
import org.apache.cassandra.service.paxos.PrepareResponse;
import org.apache.cassandra.service.paxos.v1.PrepareVerbHandler;
import org.apache.cassandra.service.paxos.v1.ProposeVerbHandler;
import org.apache.cassandra.streaming.ReplicationDoneVerbHandler;
import org.apache.cassandra.utils.ReflectionUtils;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.UUIDSerializer;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.concurrent.Stage.*;
import static org.apache.cassandra.net.VerbTimeouts.*;
import static org.apache.cassandra.net.Verb.Kind.*;
import static org.apache.cassandra.net.Verb.Priority.*;
import static org.apache.cassandra.net.ResponseHandlerSupplier.RESPONSE_HANDLER;

/**
 * Note that priorities except P0 are presently unused.  P0 corresponds to urgent, i.e. what used to be the "Gossip" connection.
 */
@SuppressWarnings("Convert2MethodRef") // we must defer all initialisation, which includes e.g. taking a method reference to a static object/singleton, which this inspection does not disambiguate
public enum Verb
{
    MUTATION_RSP           (60,  P1, writeTimeout,    REQUEST_RESPONSE,  () -> NoPayload.serializer,                 RESPONSE_HANDLER                             ),
    MUTATION_REQ           (0,   P3, writeTimeout,    MUTATION,          () -> Mutation.serializer,                  () -> MutationVerbHandler.instance,        MUTATION_RSP        ),
    HINT_RSP               (61,  P1, writeTimeout,    REQUEST_RESPONSE,  () -> NoPayload.serializer,                 RESPONSE_HANDLER                             ),
    HINT_REQ               (1,   P4, writeTimeout,    MUTATION,          () -> HintMessage.serializer,               () -> HintVerbHandler.instance,            HINT_RSP            ),
    READ_REPAIR_RSP        (62,  P1, writeTimeout,    REQUEST_RESPONSE,  () -> NoPayload.serializer,                 RESPONSE_HANDLER                             ),
    READ_REPAIR_REQ        (2,   P1, writeTimeout,    MUTATION,          () -> Mutation.serializer,                  () -> ReadRepairVerbHandler.instance,      READ_REPAIR_RSP     ),
    BATCH_STORE_RSP        (65,  P1, writeTimeout,    REQUEST_RESPONSE,  () -> NoPayload.serializer,                 RESPONSE_HANDLER                             ),
    BATCH_STORE_REQ        (5,   P3, writeTimeout,    MUTATION,          () -> Batch.serializer,                     () -> BatchStoreVerbHandler.instance,      BATCH_STORE_RSP     ),
    BATCH_REMOVE_RSP       (66,  P1, writeTimeout,    REQUEST_RESPONSE,  () -> NoPayload.serializer,                 RESPONSE_HANDLER                             ),
    BATCH_REMOVE_REQ       (6,   P3, writeTimeout,    MUTATION,          () -> TimeUUID.Serializer.instance,         () -> BatchRemoveVerbHandler.instance,     BATCH_REMOVE_RSP    ),

    PAXOS_PREPARE_RSP      (93,  P2, writeTimeout,    REQUEST_RESPONSE,  () -> PrepareResponse.serializer,           RESPONSE_HANDLER                             ),
    PAXOS_PREPARE_REQ      (33,  P2, writeTimeout,    MUTATION,          () -> Commit.serializer,                    () -> PrepareVerbHandler.instance,         PAXOS_PREPARE_RSP   ),
    PAXOS_PROPOSE_RSP      (94,  P2, writeTimeout,    REQUEST_RESPONSE,  () -> BooleanSerializer.serializer,         RESPONSE_HANDLER                             ),
    PAXOS_PROPOSE_REQ      (34,  P2, writeTimeout,    MUTATION,          () -> Commit.serializer,                    () -> ProposeVerbHandler.instance,         PAXOS_PROPOSE_RSP   ),
    PAXOS_COMMIT_RSP       (95,  P2, writeTimeout,    REQUEST_RESPONSE,  () -> NoPayload.serializer,                 RESPONSE_HANDLER                             ),
    PAXOS_COMMIT_REQ       (35,  P2, writeTimeout,    MUTATION,          () -> Agreed.serializer,                    () -> PaxosCommit.requestHandler,          PAXOS_COMMIT_RSP    ),

    TRUNCATE_RSP           (79,  P0, truncateTimeout, REQUEST_RESPONSE,  () -> TruncateResponse.serializer,          RESPONSE_HANDLER                             ),
    TRUNCATE_REQ           (19,  P0, truncateTimeout, MUTATION,          () -> TruncateRequest.serializer,           () -> TruncateVerbHandler.instance,        TRUNCATE_RSP        ),

    COUNTER_MUTATION_RSP   (84,  P1, counterTimeout,  REQUEST_RESPONSE,  () -> NoPayload.serializer,                 RESPONSE_HANDLER                             ),
    COUNTER_MUTATION_REQ   (24,  P2, counterTimeout,  COUNTER_MUTATION,  () -> CounterMutation.serializer,           () -> CounterMutationVerbHandler.instance, COUNTER_MUTATION_RSP),

    READ_RSP               (63,  P2, readTimeout,     REQUEST_RESPONSE,  () -> ReadResponse.serializer,              RESPONSE_HANDLER                             ),
    READ_REQ               (3,   P3, readTimeout,     READ,              () -> ReadCommand.serializer,               () -> ReadCommandVerbHandler.instance,     READ_RSP            ),
    RANGE_RSP              (69,  P2, rangeTimeout,    REQUEST_RESPONSE,  () -> ReadResponse.serializer,              RESPONSE_HANDLER                             ),
    RANGE_REQ              (9,   P3, rangeTimeout,    READ,              () -> ReadCommand.serializer,               () -> ReadCommandVerbHandler.instance,     RANGE_RSP           ),

    GOSSIP_DIGEST_SYN      (14,  P0, longTimeout,     GOSSIP,            () -> GossipDigestSyn.serializer,           () -> GossipDigestSynVerbHandler.instance                      ),
    GOSSIP_DIGEST_ACK      (15,  P0, longTimeout,     GOSSIP,            () -> GossipDigestAck.serializer,           () -> GossipDigestAckVerbHandler.instance                      ),
    GOSSIP_DIGEST_ACK2     (16,  P0, longTimeout,     GOSSIP,            () -> GossipDigestAck2.serializer,          () -> GossipDigestAck2VerbHandler.instance                     ),
    GOSSIP_SHUTDOWN        (29,  P0, rpcTimeout,      GOSSIP,            () -> NoPayload.serializer,                 () -> GossipShutdownVerbHandler.instance                       ),

    ECHO_RSP               (91,  P0, rpcTimeout,      GOSSIP,            () -> NoPayload.serializer,                 RESPONSE_HANDLER                             ),
    ECHO_REQ               (31,  P0, rpcTimeout,      GOSSIP,            () -> NoPayload.serializer,                 () -> EchoVerbHandler.instance,            ECHO_RSP            ),
    PING_RSP               (97,  P1, pingTimeout,     GOSSIP,            () -> NoPayload.serializer,                 RESPONSE_HANDLER                             ),
    PING_REQ               (37,  P1, pingTimeout,     GOSSIP,            () -> PingRequest.serializer,               () -> PingVerbHandler.instance,            PING_RSP            ),

    // P1 because messages can be arbitrarily large or aren't crucial
    SCHEMA_PUSH_RSP        (98,  P1, rpcTimeout,      MIGRATION,         () -> NoPayload.serializer,                 RESPONSE_HANDLER                             ),
    SCHEMA_PUSH_REQ        (18,  P1, rpcTimeout,      MIGRATION,         () -> SchemaMutationsSerializer.instance,   () -> SchemaPushVerbHandler.instance,      SCHEMA_PUSH_RSP     ),
    SCHEMA_PULL_RSP        (88,  P1, rpcTimeout,      MIGRATION,         () -> SchemaMutationsSerializer.instance,   RESPONSE_HANDLER                             ),
    SCHEMA_PULL_REQ        (28,  P1, rpcTimeout,      MIGRATION,         () -> NoPayload.serializer,                 () -> SchemaPullVerbHandler.instance,      SCHEMA_PULL_RSP     ),
    SCHEMA_VERSION_RSP     (80,  P1, rpcTimeout,      MIGRATION,         () -> UUIDSerializer.serializer,            RESPONSE_HANDLER                             ),
    SCHEMA_VERSION_REQ     (20,  P1, rpcTimeout,      MIGRATION,         () -> NoPayload.serializer,                 () -> SchemaVersionVerbHandler.instance,   SCHEMA_VERSION_RSP  ),

    // repair; mostly doesn't use callbacks and sends responses as their own request messages, with matching sessions by uuid; should eventually harmonize and make idiomatic
    REPAIR_RSP             (100, P1, repairTimeout,   REQUEST_RESPONSE,  () -> NoPayload.serializer,                 RESPONSE_HANDLER                             ),
    VALIDATION_RSP         (102, P1, longTimeout,     ANTI_ENTROPY,      () -> ValidationResponse.serializer,        () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          ),
    VALIDATION_REQ         (101, P1, repairTimeout,   ANTI_ENTROPY,      () -> ValidationRequest.serializer,         () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          ),
    SYNC_RSP               (104, P1, repairTimeout,   ANTI_ENTROPY,      () -> SyncResponse.serializer,              () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          ),
    SYNC_REQ               (103, P1, repairTimeout,   ANTI_ENTROPY,      () -> SyncRequest.serializer,               () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          ),
    PREPARE_MSG            (105, P1, repairTimeout,   ANTI_ENTROPY,      () -> PrepareMessage.serializer,            () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          ),
    SNAPSHOT_MSG           (106, P1, repairTimeout,   ANTI_ENTROPY,      () -> SnapshotMessage.serializer,           () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          ),
    CLEANUP_MSG            (107, P1, repairTimeout,   ANTI_ENTROPY,      () -> CleanupMessage.serializer,            () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          ),
    PREPARE_CONSISTENT_RSP (109, P1, repairTimeout,   ANTI_ENTROPY,      () -> PrepareConsistentResponse.serializer, () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          ),
    PREPARE_CONSISTENT_REQ (108, P1, repairTimeout,   ANTI_ENTROPY,      () -> PrepareConsistentRequest.serializer,  () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          ),
    FINALIZE_PROPOSE_MSG   (110, P1, repairTimeout,   ANTI_ENTROPY,      () -> FinalizePropose.serializer,           () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          ),
    FINALIZE_PROMISE_MSG   (111, P1, repairTimeout,   ANTI_ENTROPY,      () -> FinalizePromise.serializer,           () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          ),
    FINALIZE_COMMIT_MSG    (112, P1, repairTimeout,   ANTI_ENTROPY,      () -> FinalizeCommit.serializer,            () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          ),
    FAILED_SESSION_MSG     (113, P1, repairTimeout,   ANTI_ENTROPY,      () -> FailSession.serializer,               () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          ),
    STATUS_RSP             (115, P1, repairTimeout,   ANTI_ENTROPY,      () -> StatusResponse.serializer,            () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          ),
    STATUS_REQ             (114, P1, repairTimeout,   ANTI_ENTROPY,      () -> StatusRequest.serializer,             () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          ),

    REPLICATION_DONE_RSP   (82,  P0, rpcTimeout,      MISC,              () -> NoPayload.serializer,                 RESPONSE_HANDLER                             ),
    REPLICATION_DONE_REQ   (22,  P0, rpcTimeout,      MISC,              () -> NoPayload.serializer,                 () -> ReplicationDoneVerbHandler.instance, REPLICATION_DONE_RSP),
    SNAPSHOT_RSP           (87,  P0, rpcTimeout,      MISC,              () -> NoPayload.serializer,                 RESPONSE_HANDLER                             ),
    SNAPSHOT_REQ           (27,  P0, rpcTimeout,      MISC,              () -> SnapshotCommand.serializer,           () -> SnapshotVerbHandler.instance,        SNAPSHOT_RSP        ),

    PAXOS2_COMMIT_REMOTE_REQ         (38, P2, writeTimeout,  MUTATION,          () -> Mutation.serializer,                     () -> MutationVerbHandler.instance,                          MUTATION_RSP                     ),
    PAXOS2_COMMIT_REMOTE_RSP         (39, P2, writeTimeout,  REQUEST_RESPONSE,  () -> NoPayload.serializer,                    RESPONSE_HANDLER                                                            ),
    PAXOS2_PREPARE_RSP               (50, P2, writeTimeout,  REQUEST_RESPONSE,  () -> PaxosPrepare.responseSerializer,         RESPONSE_HANDLER                                                            ),
    PAXOS2_PREPARE_REQ               (40, P2, writeTimeout,  MUTATION,          () -> PaxosPrepare.requestSerializer,          () -> PaxosPrepare.requestHandler,                           PAXOS2_PREPARE_RSP               ),
    PAXOS2_PREPARE_REFRESH_RSP       (51, P2, writeTimeout,  REQUEST_RESPONSE,  () -> PaxosPrepareRefresh.responseSerializer,  RESPONSE_HANDLER                                                            ),
    PAXOS2_PREPARE_REFRESH_REQ       (41, P2, writeTimeout,  MUTATION,          () -> PaxosPrepareRefresh.requestSerializer,   () -> PaxosPrepareRefresh.requestHandler,                    PAXOS2_PREPARE_REFRESH_RSP       ),
    PAXOS2_PROPOSE_RSP               (52, P2, writeTimeout,  REQUEST_RESPONSE,  () -> PaxosPropose.responseSerializer,         RESPONSE_HANDLER                                                            ),
    PAXOS2_PROPOSE_REQ               (42, P2, writeTimeout,  MUTATION,          () -> PaxosPropose.requestSerializer,          () -> PaxosPropose.requestHandler,                           PAXOS2_PROPOSE_RSP               ),
    PAXOS2_COMMIT_AND_PREPARE_RSP    (53, P2, writeTimeout,  REQUEST_RESPONSE,  () -> PaxosPrepare.responseSerializer,         RESPONSE_HANDLER                                                            ),
    PAXOS2_COMMIT_AND_PREPARE_REQ    (43, P2, writeTimeout,  MUTATION,          () -> PaxosCommitAndPrepare.requestSerializer, () -> PaxosCommitAndPrepare.requestHandler,                  PAXOS2_COMMIT_AND_PREPARE_RSP    ),
    PAXOS2_REPAIR_RSP                (54, P2, writeTimeout,  PAXOS_REPAIR,      () -> PaxosRepair.responseSerializer,          RESPONSE_HANDLER                                                            ),
    PAXOS2_REPAIR_REQ                (44, P2, writeTimeout,  PAXOS_REPAIR,      () -> PaxosRepair.requestSerializer,           () -> PaxosRepair.requestHandler,                            PAXOS2_REPAIR_RSP                ),
    PAXOS2_CLEANUP_START_PREPARE_RSP (55, P2, repairTimeout, PAXOS_REPAIR,      () -> PaxosCleanupHistory.serializer,          RESPONSE_HANDLER                                                            ),
    PAXOS2_CLEANUP_START_PREPARE_REQ (45, P2, repairTimeout, PAXOS_REPAIR,      () -> PaxosStartPrepareCleanup.serializer,     () -> PaxosStartPrepareCleanup.verbHandler,                  PAXOS2_CLEANUP_START_PREPARE_RSP ),
    PAXOS2_CLEANUP_RSP               (56, P2, repairTimeout, PAXOS_REPAIR,      () -> NoPayload.serializer,                    RESPONSE_HANDLER                                                            ),
    PAXOS2_CLEANUP_REQ               (46, P2, repairTimeout, PAXOS_REPAIR,      () -> PaxosCleanupRequest.serializer,          () -> PaxosCleanupRequest.verbHandler,                       PAXOS2_CLEANUP_RSP               ),
    PAXOS2_CLEANUP_RSP2              (57, P2, repairTimeout, PAXOS_REPAIR,      () -> PaxosCleanupResponse.serializer,         () -> PaxosCleanupResponse.verbHandler                                                        ),
    PAXOS2_CLEANUP_FINISH_PREPARE_RSP(58, P2, repairTimeout, PAXOS_REPAIR,      () -> NoPayload.serializer,                    RESPONSE_HANDLER                                                            ),
    PAXOS2_CLEANUP_FINISH_PREPARE_REQ(47, P2, repairTimeout, IMMEDIATE,         () -> PaxosCleanupHistory.serializer,          () -> PaxosFinishPrepareCleanup.verbHandler,                 PAXOS2_CLEANUP_FINISH_PREPARE_RSP),
    PAXOS2_CLEANUP_COMPLETE_RSP      (59, P2, repairTimeout, PAXOS_REPAIR,      () -> NoPayload.serializer,                    RESPONSE_HANDLER                                                            ),
    PAXOS2_CLEANUP_COMPLETE_REQ      (48, P2, repairTimeout, PAXOS_REPAIR,      () -> PaxosCleanupComplete.serializer,         () -> PaxosCleanupComplete.verbHandler,                      PAXOS2_CLEANUP_COMPLETE_RSP      ),

    // accord
    ACCORD_PREACCEPT_RSP            (121, P2, writeTimeout,    REQUEST_RESPONSE,  () -> PreacceptSerializers.reply,           RESPONSE_HANDLER),
    ACCORD_PREACCEPT_REQ            (120, P2, writeTimeout,    ACCORD,            () -> PreacceptSerializers.request,         () -> AccordService.instance().verbHandler(),       ACCORD_PREACCEPT_RSP),

    ACCORD_ACCEPT_RSP               (124, P2, writeTimeout,    REQUEST_RESPONSE,  () -> AcceptSerializers.reply,              RESPONSE_HANDLER),
    ACCORD_ACCEPT_REQ               (122, P2, writeTimeout,    ACCORD,            () -> AcceptSerializers.request,            () -> AccordService.instance().verbHandler(),       ACCORD_ACCEPT_RSP   ),
    ACCORD_ACCEPT_INVALIDATE_REQ    (123, P2, writeTimeout,    ACCORD,            () -> AcceptSerializers.invalidate,         () -> AccordService.instance().verbHandler(),       ACCORD_ACCEPT_RSP   ),

    ACCORD_READ_RSP                 (128, P2, writeTimeout,    REQUEST_RESPONSE,  () -> ReadDataSerializers.reply,            RESPONSE_HANDLER),
    ACCORD_READ_REQ                 (127, P2, writeTimeout,    ACCORD,            () -> ReadDataSerializers.request,          () -> AccordService.instance().verbHandler(),       ACCORD_READ_RSP     ),
    ACCORD_COMMIT_REQ               (125, P2, writeTimeout,    ACCORD,            () -> CommitSerializers.request,            () -> AccordService.instance().verbHandler(), ACCORD_READ_RSP     ),
    ACCORD_COMMIT_INVALIDATE_REQ    (126, P2, writeTimeout,    ACCORD,            () -> CommitSerializers.invalidate,         () -> AccordService.instance().verbHandler()),

    ACCORD_APPLY_RSP                (130, P2, writeTimeout,    REQUEST_RESPONSE,  () -> ApplySerializers.reply,               RESPONSE_HANDLER),
    ACCORD_APPLY_REQ                (129, P2, writeTimeout,    ACCORD,            () -> ApplySerializers.request,             () -> AccordService.instance().verbHandler(), ACCORD_APPLY_RSP),
    ACCORD_APPLY_AND_CHECK_RSP      (132, P2, writeTimeout,    REQUEST_RESPONSE,  () -> ApplyAndCheckSerializers.reply,       RESPONSE_HANDLER),
    ACCORD_APPLY_AND_CHECK_REQ      (131, P2, writeTimeout,    ACCORD,            () -> ApplyAndCheckSerializers.request,     () -> AccordService.instance().verbHandler(), ACCORD_APPLY_AND_CHECK_RSP),

    ACCORD_RECOVER_RSP              (134, P2, writeTimeout,    REQUEST_RESPONSE,  () -> RecoverySerializers.reply,            RESPONSE_HANDLER),
    ACCORD_RECOVER_REQ              (133, P2, writeTimeout,    ACCORD,            () -> RecoverySerializers.request,          () -> AccordService.instance().verbHandler(),       ACCORD_RECOVER_RSP  ),
    ACCORD_BEGIN_INVALIDATE_RSP     (136, P2, writeTimeout,    REQUEST_RESPONSE,  () -> BeginInvalidateSerializers.reply,     RESPONSE_HANDLER),
    ACCORD_BEGIN_INVALIDATE_REQ     (135, P2, writeTimeout,    ACCORD,            () -> BeginInvalidateSerializers.request,   () -> AccordService.instance().verbHandler(),ACCORD_BEGIN_INVALIDATE_RSP),
    ACCORD_WAIT_COMMIT_RSP          (138, P2, writeTimeout,    REQUEST_RESPONSE,  () -> WaitOnCommitSerializer.reply,         RESPONSE_HANDLER),
    ACCORD_WAIT_COMMIT_REQ          (137, P2, writeTimeout,    ACCORD,            () -> WaitOnCommitSerializer.request,       () -> AccordService.instance().verbHandler(),     ACCORD_WAIT_COMMIT_RSP),

    ACCORD_INFORM_OF_TXN_RSP        (140, P2, writeTimeout, REQUEST_RESPONSE,     () -> InformOfTxnSerializers.reply,         RESPONSE_HANDLER),
    ACCORD_INFORM_OF_TXN_REQ        (139, P2, writeTimeout,    ACCORD,            () -> InformOfTxnSerializers.request,       () -> AccordService.instance().verbHandler(),   ACCORD_INFORM_OF_TXN_RSP),

    ACCORD_INFORM_OF_PERSISTENCE_RSP(142, P2, writeTimeout, REQUEST_RESPONSE,     () -> InformOfTxnSerializers.reply,         RESPONSE_HANDLER),
    ACCORD_INFORM_OF_PERSIETENCE_REQ(141, P2, writeTimeout, ACCORD,               () -> InformOfPersistenceSerializers.request, () -> AccordService.instance().verbHandler(), ACCORD_INFORM_OF_TXN_RSP),

    ACCORD_CHECK_STATUS_RSP         (144, P2, writeTimeout, REQUEST_RESPONSE,     () -> CheckStatusSerializers.reply,         RESPONSE_HANDLER),
    ACCORD_CHECK_STATUS_REQ         (143, P2, writeTimeout, ACCORD,               () -> CheckStatusSerializers.request,       () -> AccordService.instance().verbHandler(), ACCORD_CHECK_STATUS_RSP),

    // generic failure response
    FAILURE_RSP            (99,  P0, noTimeout,       REQUEST_RESPONSE,  () -> RequestFailureReason.serializer,      RESPONSE_HANDLER                             ),

    // dummy verbs
    _TRACE                 (30,  P1, rpcTimeout,      TRACING,           () -> NoPayload.serializer,                 () -> null                                                     ),
    _SAMPLE                (49,  P1, rpcTimeout,      INTERNAL_RESPONSE, () -> NoPayload.serializer,                 () -> null                                                     ),
    _TEST_1                (10,  P0, writeTimeout,    IMMEDIATE,         () -> NoPayload.serializer,                 () -> null                                                     ),
    _TEST_2                (11,  P1, rpcTimeout,      IMMEDIATE,         () -> NoPayload.serializer,                 () -> null                                                     ),

    @Deprecated
    REQUEST_RSP            (4,   P1, rpcTimeout,      REQUEST_RESPONSE,  () -> null,                                 RESPONSE_HANDLER                             ),
    @Deprecated
    INTERNAL_RSP           (23,  P1, rpcTimeout,      INTERNAL_RESPONSE, () -> null,                                 RESPONSE_HANDLER                             ),

    // largest used ID: 116

    // CUSTOM VERBS
    UNUSED_CUSTOM_VERB     (CUSTOM,
                            0,   P1, rpcTimeout,      INTERNAL_RESPONSE, () -> null,                                 () -> null                                                     ),
    ;

    public static final List<Verb> VERBS = ImmutableList.copyOf(Verb.values());

    public enum Priority
    {
        P0,  // sends on the urgent connection (i.e. for Gossip, Echo)
        P1,  // small or empty responses
        P2,  // larger messages that can be dropped but who have a larger impact on system stability (e.g. READ_REPAIR, READ_RSP)
        P3,
        P4
    }

    public enum Kind
    {
        NORMAL,
        CUSTOM
    }

    public final int id;
    public final Priority priority;
    public final Stage stage;
    public final Kind kind;

    /**
     * Messages we receive from peers have a Verb that tells us what kind of message it is.
     * Most of the time, this is enough to determine how to deserialize the message payload.
     * The exception is the REQUEST_RSP verb, which just means "a response to something you told me to do."
     * Traditionally, this was fine since each VerbHandler knew what type of payload it expected, and
     * handled the deserialization itself.  Now that we do that in ITC, to avoid the extra copy to an
     * intermediary byte[] (See CASSANDRA-3716), we need to wire that up to the CallbackInfo object
     * (see below).
     *
     * NOTE: we use a Supplier to avoid loading the dependent classes until necessary.
     */
    private final Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> serializer;
    private final Supplier<? extends IVerbHandler<?>> handler;

    public final Verb responseVerb;

    private final ToLongFunction<TimeUnit> expiration;


    /**
     * Verbs it's okay to drop if the request has been queued longer than the request timeout.  These
     * all correspond to client requests or something triggered by them; we don't want to
     * drop internal messages like bootstrap or repair notifications.
     */
    Verb(int id, Priority priority, ToLongFunction<TimeUnit> expiration, Stage stage, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> serializer, Supplier<? extends IVerbHandler<?>> handler)
    {
        this(NORMAL, id, priority, expiration, stage, serializer, handler, null, null);
    }

    Verb(int id, Priority priority, ToLongFunction<TimeUnit> expiration, Stage stage, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> serializer, Supplier<? extends IVerbHandler<?>> handler, Predicate<?> isFinalReply)
    {
        this(NORMAL, id, priority, expiration, stage, serializer, handler, null, isFinalReply);
    }

    Verb(int id, Priority priority, ToLongFunction<TimeUnit> expiration, Stage stage, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> serializer, Supplier<? extends IVerbHandler<?>> handler, Verb responseVerb)
    {
        this(NORMAL, id, priority, expiration, stage, serializer, handler, responseVerb, null);
    }

    Verb(Kind kind, int id, Priority priority, ToLongFunction<TimeUnit> expiration, Stage stage, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> serializer, Supplier<? extends IVerbHandler<?>> handler)
    {
        this(kind, id, priority, expiration, stage, serializer, handler, null, null);
    }

    Verb(Kind kind, int id, Priority priority, ToLongFunction<TimeUnit> expiration, Stage stage, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> serializer, Supplier<? extends IVerbHandler<?>> handler, Predicate<?> isFinalReply)
    {
        this(kind, id, priority, expiration, stage, serializer, handler, null, isFinalReply);
    }

    Verb(Kind kind, int id, Priority priority, ToLongFunction<TimeUnit> expiration, Stage stage, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> serializer, Supplier<? extends IVerbHandler<?>> handler, Verb responseVerb, Predicate<?> isFinalReply)
    {
        this.stage = stage;
        if (id < 0)
            throw new IllegalArgumentException("Verb id must be non-negative, got " + id + " for verb " + name());

        if (kind == CUSTOM)
        {
            if (id > MAX_CUSTOM_VERB_ID)
                throw new AssertionError("Invalid custom verb id " + id + " - we only allow custom ids between 0 and " + MAX_CUSTOM_VERB_ID);
            this.id = idForCustomVerb(id);
        }
        else
        {
            if (id > CUSTOM_VERB_START - MAX_CUSTOM_VERB_ID)
                throw new AssertionError("Invalid verb id " + id + " - we only allow ids between 0 and " + (CUSTOM_VERB_START - MAX_CUSTOM_VERB_ID));
            this.id = id;
        }
        this.priority = priority;
        this.serializer = serializer;
        this.handler = handler;
        this.responseVerb = responseVerb;
        this.expiration = expiration;
        this.kind = kind;
    }

    public <In, Out> IVersionedAsymmetricSerializer<In, Out> serializer()
    {
        return (IVersionedAsymmetricSerializer<In, Out>) serializer.get();
    }

    public <T> IVerbHandler<T> handler()
    {
        return (IVerbHandler<T>) handler.get();
    }

    public long expiresAtNanos(long nowNanos)
    {
        return nowNanos + expiresAfterNanos();
    }

    public long expiresAfterNanos()
    {
        return expiration.applyAsLong(NANOSECONDS);
    }

    // this is a little hacky, but reduces the number of parameters up top
    public boolean isResponse()
    {
        return handler == RESPONSE_HANDLER;
    }

    Verb toPre40Verb()
    {
        if (!isResponse())
            return this;
        if (priority == P0)
            return INTERNAL_RSP;
        return REQUEST_RSP;
    }

    @VisibleForTesting
    Supplier<? extends IVerbHandler<?>> unsafeSetHandler(Supplier<? extends IVerbHandler<?>> handler) throws NoSuchFieldException, IllegalAccessException
    {
        Supplier<? extends IVerbHandler<?>> original = this.handler;
        Field field = Verb.class.getDeclaredField("handler");
        field.setAccessible(true);
        Field modifiers = ReflectionUtils.getModifiersField();
        modifiers.setAccessible(true);
        modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(this, handler);
        return original;
    }

    @VisibleForTesting
    public Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> unsafeSetSerializer(Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> serializer) throws NoSuchFieldException, IllegalAccessException
    {
        Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> original = this.serializer;
        Field field = Verb.class.getDeclaredField("serializer");
        field.setAccessible(true);
        Field modifiers = ReflectionUtils.getModifiersField();
        modifiers.setAccessible(true);
        modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(this, serializer);
        return original;
    }

    @VisibleForTesting
    ToLongFunction<TimeUnit> unsafeSetExpiration(ToLongFunction<TimeUnit> expiration) throws NoSuchFieldException, IllegalAccessException
    {
        ToLongFunction<TimeUnit> original = this.expiration;
        Field field = Verb.class.getDeclaredField("expiration");
        field.setAccessible(true);
        Field modifiers = ReflectionUtils.getModifiersField();
        modifiers.setAccessible(true);
        modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(this, expiration);
        return original;
    }

    // This is the largest number we can store in 2 bytes using VIntCoding (1 bit per byte is used to indicate if there is more data coming).
    // When generating ids we count *down* from this number
    private static final int CUSTOM_VERB_START = (1 << (7 * 2)) - 1;

    // Sanity check for the custom verb ids - avoids someone mistakenly adding a custom verb id close to the normal verbs which
    // could cause a conflict later when new normal verbs are added.
    private static final int MAX_CUSTOM_VERB_ID = 1000;

    private static final Verb[] idToVerbMap;
    private static final Verb[] idToCustomVerbMap;
    private static final int minCustomId;

    static
    {
        Verb[] verbs = values();
        int max = -1;
        int minCustom = Integer.MAX_VALUE;
        for (Verb v : verbs)
        {
            switch (v.kind)
            {
                case NORMAL:
                    max = Math.max(v.id, max);
                    break;
                case CUSTOM:
                    minCustom = Math.min(v.id, minCustom);
                    break;
                default:
                    throw new AssertionError("Unsupported Verb Kind: " + v.kind + " for verb " + v);
            }
        }
        minCustomId = minCustom;

        if (minCustom <= max)
            throw new IllegalStateException("Overlapping verb ids are not allowed");

        Verb[] idMap = new Verb[max + 1];
        int customCount = minCustom < Integer.MAX_VALUE ? CUSTOM_VERB_START - minCustom : 0;
        Verb[] customIdMap = new Verb[customCount + 1];
        for (Verb v : verbs)
        {
            switch (v.kind)
            {
                case NORMAL:
                    if (idMap[v.id] != null)
                        throw new IllegalArgumentException("cannot have two verbs that map to the same id: " + v + " and " + idMap[v.id]);
                    idMap[v.id] = v;
                    break;
                case CUSTOM:
                    int relativeId = idForCustomVerb(v.id);
                    if (customIdMap[relativeId] != null)
                        throw new IllegalArgumentException("cannot have two custom verbs that map to the same id: " + v + " and " + customIdMap[relativeId]);
                    customIdMap[relativeId] = v;
                    break;
                default:
                    throw new AssertionError("Unsupported Verb Kind: " + v.kind + " for verb " + v);
            }
        }

        idToVerbMap = idMap;
        idToCustomVerbMap = customIdMap;
    }

    public static Verb fromId(int id)
    {
        Verb[] verbs = idToVerbMap;
        if (id >= minCustomId)
        {
            id = idForCustomVerb(id);
            verbs = idToCustomVerbMap;
        }
        Verb verb = id >= 0 && id < verbs.length ? verbs[id] : null;
        if (verb == null)
            throw new IllegalArgumentException("Unknown verb id " + id);
        return verb;
    }

    /**
     * calculate an id for a custom verb
     */
    private static int idForCustomVerb(int id)
    {
        return CUSTOM_VERB_START - id;
    }
}

@SuppressWarnings("unused")
class VerbTimeouts
{
    static final ToLongFunction<TimeUnit> rpcTimeout      = DatabaseDescriptor::getRpcTimeout;
    static final ToLongFunction<TimeUnit> writeTimeout    = DatabaseDescriptor::getWriteRpcTimeout;
    static final ToLongFunction<TimeUnit> readTimeout     = DatabaseDescriptor::getReadRpcTimeout;
    static final ToLongFunction<TimeUnit> rangeTimeout    = DatabaseDescriptor::getRangeRpcTimeout;
    static final ToLongFunction<TimeUnit> counterTimeout  = DatabaseDescriptor::getCounterWriteRpcTimeout;
    static final ToLongFunction<TimeUnit> truncateTimeout = DatabaseDescriptor::getTruncateRpcTimeout;
    static final ToLongFunction<TimeUnit> repairTimeout   = DatabaseDescriptor::getRepairRpcTimeout;
    static final ToLongFunction<TimeUnit> pingTimeout     = DatabaseDescriptor::getPingTimeout;
    static final ToLongFunction<TimeUnit> longTimeout     = units -> Math.max(DatabaseDescriptor.getRpcTimeout(units), units.convert(5L, TimeUnit.MINUTES));
    static final ToLongFunction<TimeUnit> noTimeout       = units -> {  throw new IllegalStateException(); };
}

class ResponseHandlerSupplier
{
    static final Supplier<IVerbHandler<?>> RESPONSE_HANDLER = () -> ResponseVerbHandler.instance;
}