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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.batchlog.Batch;
import org.apache.cassandra.batchlog.BatchRemoveVerbHandler;
import org.apache.cassandra.batchlog.BatchStoreVerbHandler;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.CounterMutationVerbHandler;
import org.apache.cassandra.db.MultiRangeReadCommand;
import org.apache.cassandra.db.MultiRangeReadResponse;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationVerbHandler;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadRepairVerbHandler;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SnapshotCommand;
import org.apache.cassandra.db.TruncateRequest;
import org.apache.cassandra.db.TruncateResponse;
import org.apache.cassandra.db.TruncateVerbHandler;
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
import org.apache.cassandra.repair.messages.SyncRequest;
import org.apache.cassandra.repair.messages.SyncResponse;
import org.apache.cassandra.repair.messages.ValidationRequest;
import org.apache.cassandra.repair.messages.ValidationResponse;
import org.apache.cassandra.schema.SchemaMutationsSerializer;
import org.apache.cassandra.schema.SchemaPullVerbHandler;
import org.apache.cassandra.schema.SchemaPushVerbHandler;
import org.apache.cassandra.schema.SchemaVersionVerbHandler;
import org.apache.cassandra.service.EchoVerbHandler;
import org.apache.cassandra.service.SnapshotVerbHandler;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.CommitVerbHandler;
import org.apache.cassandra.service.paxos.PrepareResponse;
import org.apache.cassandra.service.paxos.PrepareVerbHandler;
import org.apache.cassandra.service.paxos.ProposeVerbHandler;
import org.apache.cassandra.streaming.ReplicationDoneVerbHandler;
import org.apache.cassandra.utils.BooleanSerializer;
import org.apache.cassandra.utils.UUIDSerializer;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.concurrent.Stage.ANTI_ENTROPY;
import static org.apache.cassandra.concurrent.Stage.COUNTER_MUTATION;
import static org.apache.cassandra.concurrent.Stage.GOSSIP;
import static org.apache.cassandra.concurrent.Stage.IMMEDIATE;
import static org.apache.cassandra.concurrent.Stage.INTERNAL_RESPONSE;
import static org.apache.cassandra.concurrent.Stage.MIGRATION;
import static org.apache.cassandra.concurrent.Stage.MISC;
import static org.apache.cassandra.concurrent.Stage.MUTATION;
import static org.apache.cassandra.concurrent.Stage.READ;
import static org.apache.cassandra.concurrent.Stage.REQUEST_RESPONSE;
import static org.apache.cassandra.concurrent.Stage.TRACING;
import static org.apache.cassandra.net.Verb.Kind.CUSTOM;
import static org.apache.cassandra.net.Verb.Kind.NORMAL;
import static org.apache.cassandra.net.Verb.Priority.P0;
import static org.apache.cassandra.net.Verb.Priority.P1;
import static org.apache.cassandra.net.Verb.Priority.P2;
import static org.apache.cassandra.net.Verb.Priority.P3;
import static org.apache.cassandra.net.Verb.Priority.P4;
import static org.apache.cassandra.net.VerbTimeouts.counterTimeout;
import static org.apache.cassandra.net.VerbTimeouts.hintTimeout;
import static org.apache.cassandra.net.VerbTimeouts.longTimeout;
import static org.apache.cassandra.net.VerbTimeouts.noTimeout;
import static org.apache.cassandra.net.VerbTimeouts.pingTimeout;
import static org.apache.cassandra.net.VerbTimeouts.prepareTimeout;
import static org.apache.cassandra.net.VerbTimeouts.rangeTimeout;
import static org.apache.cassandra.net.VerbTimeouts.readTimeout;
import static org.apache.cassandra.net.VerbTimeouts.repairMsgTimeout;
import static org.apache.cassandra.net.VerbTimeouts.rpcTimeout;
import static org.apache.cassandra.net.VerbTimeouts.truncateTimeout;
import static org.apache.cassandra.net.VerbTimeouts.writeTimeout;

/**
 * Note that priorities except P0 are presently unused.  P0 corresponds to urgent, i.e. what used to be the "Gossip" connection.
 */
public class Verb
{
    private static final List<Verb> verbs = new ArrayList<>();

    public static List<Verb> getValues()
    {
        return ImmutableList.copyOf(verbs);
    }

    public static Verb MUTATION_RSP           = new Verb("MUTATION_RSP",           60,  P1, writeTimeout,    REQUEST_RESPONSE,  () -> NoPayload.serializer,                 () -> ResponseVerbHandler.instance);
    public static Verb MUTATION_REQ           = new Verb("MUTATION_REQ",           0,   P3, writeTimeout,    MUTATION,          () -> Mutation.serializer,                  () -> MutationVerbHandler.instance,        MUTATION_RSP);
    public static Verb HINT_RSP               = new Verb("HINT_RSP",               61,  P1, hintTimeout,     REQUEST_RESPONSE,  () -> NoPayload.serializer,                 () -> ResponseVerbHandler.instance                             );
    public static Verb HINT_REQ               = new Verb("HINT_REQ",               1,   P4, hintTimeout,     MUTATION,          () -> HintMessage.serializer,               () -> HintVerbHandler.instance,            HINT_RSP            );
    public static Verb READ_REPAIR_RSP        = new Verb("READ_REPAIR_RSP",        62,  P1, writeTimeout,    REQUEST_RESPONSE,  () -> NoPayload.serializer,                 () -> ResponseVerbHandler.instance                             );
    public static Verb READ_REPAIR_REQ        = new Verb("READ_REPAIR_REQ",        2,   P1, writeTimeout,    MUTATION,          () -> Mutation.serializer,                  () -> ReadRepairVerbHandler.instance,      READ_REPAIR_RSP     );
    public static Verb BATCH_STORE_RSP        = new Verb("BATCH_STORE_RSP",        65,  P1, writeTimeout,    REQUEST_RESPONSE,  () -> NoPayload.serializer,                 () -> ResponseVerbHandler.instance                             );
    public static Verb BATCH_STORE_REQ        = new Verb("BATCH_STORE_REQ",        5,   P3, writeTimeout,    MUTATION,          () -> Batch.serializer,                     () -> BatchStoreVerbHandler.instance,      BATCH_STORE_RSP     );
    public static Verb BATCH_REMOVE_RSP       = new Verb("BATCH_REMOVE_RSP",       66,  P1, writeTimeout,    REQUEST_RESPONSE,  () -> NoPayload.serializer,                 () -> ResponseVerbHandler.instance                             );
    public static Verb BATCH_REMOVE_REQ       = new Verb("BATCH_REMOVE_REQ",       6,   P3, writeTimeout,    MUTATION,          () -> UUIDSerializer.serializer,            () -> BatchRemoveVerbHandler.instance,     BATCH_REMOVE_RSP    );

    public static Verb PAXOS_PREPARE_RSP      = new Verb("PAXOS_PREPARE_RSP",      93,  P2, writeTimeout,    REQUEST_RESPONSE,  () -> PrepareResponse.serializer,           () -> ResponseVerbHandler.instance                             );
    public static Verb PAXOS_PREPARE_REQ      = new Verb("PAXOS_PREPARE_REQ",      33,  P2, writeTimeout,    MUTATION,          () -> Commit.serializer,                    () -> PrepareVerbHandler.instance,         PAXOS_PREPARE_RSP   );
    public static Verb PAXOS_PROPOSE_RSP      = new Verb("PAXOS_PROPOSE_RSP",      94,  P2, writeTimeout,    REQUEST_RESPONSE,  () -> BooleanSerializer.serializer,         () -> ResponseVerbHandler.instance                             );
    public static Verb PAXOS_PROPOSE_REQ      = new Verb("PAXOS_PROPOSE_REQ",      34,  P2, writeTimeout,    MUTATION,          () -> Commit.serializer,                    () -> ProposeVerbHandler.instance,         PAXOS_PROPOSE_RSP   );
    public static Verb PAXOS_COMMIT_RSP       = new Verb("PAXOS_COMMIT_RSP",       95,  P2, writeTimeout,    REQUEST_RESPONSE,  () -> NoPayload.serializer,                 () -> ResponseVerbHandler.instance                             );
    public static Verb PAXOS_COMMIT_REQ       = new Verb("PAXOS_COMMIT_REQ",       35,  P2, writeTimeout,    MUTATION,          () -> Commit.serializer,                    () -> CommitVerbHandler.instance,          PAXOS_COMMIT_RSP    );

    public static Verb TRUNCATE_RSP           = new Verb("TRUNCATE_RSP",           79,  P0, truncateTimeout, REQUEST_RESPONSE,  () -> TruncateResponse.serializer,          () -> ResponseVerbHandler.instance                             );
    public static Verb TRUNCATE_REQ           = new Verb("TRUNCATE_REQ",           19,  P0, truncateTimeout, MUTATION,          () -> TruncateRequest.serializer,           () -> TruncateVerbHandler.instance,        TRUNCATE_RSP        );

    public static Verb COUNTER_MUTATION_RSP   = new Verb("COUNTER_MUTATION_RSP",   84,  P1, counterTimeout,  REQUEST_RESPONSE,  () -> NoPayload.serializer,                 () -> ResponseVerbHandler.instance                             );
    public static Verb COUNTER_MUTATION_REQ   = new Verb("COUNTER_MUTATION_REQ",   24,  P2, counterTimeout,  COUNTER_MUTATION,  () -> CounterMutation.serializer,           () -> CounterMutationVerbHandler.instance, COUNTER_MUTATION_RSP);

    public static Verb READ_RSP               = new Verb("READ_RSP",               63,  P2, readTimeout,     REQUEST_RESPONSE,  () -> ReadResponse.serializer,              () -> ResponseVerbHandler.instance                             );
    public static Verb READ_REQ               = new Verb("READ_REQ",               3,   P3, readTimeout,     READ,              () -> ReadCommand.serializer,               () -> ReadCommandVerbHandler.instance,     READ_RSP            );
    public static Verb RANGE_RSP              = new Verb("RANGE_RSP",              69,  P2, rangeTimeout,    REQUEST_RESPONSE,  () -> ReadResponse.serializer,              () -> ResponseVerbHandler.instance                             );
    public static Verb RANGE_REQ              = new Verb("RANGE_REQ",              9,   P3, rangeTimeout,    READ,              () -> ReadCommand.serializer,               () -> ReadCommandVerbHandler.instance,     RANGE_RSP           );
    public static Verb MULTI_RANGE_RSP        = new Verb("MULTI_RANGE_RSP",        67,  P2, rangeTimeout,    REQUEST_RESPONSE,  () -> MultiRangeReadResponse.serializer,    () -> ResponseVerbHandler.instance                             );
    public static Verb MULTI_RANGE_REQ        = new Verb("MULTI_RANGE_REQ",        7,   P3, rangeTimeout,    READ,              () -> MultiRangeReadCommand.serializer,     () -> ReadCommandVerbHandler.instance,     MULTI_RANGE_RSP     );

    public static Verb GOSSIP_DIGEST_SYN      = new Verb("GOSSIP_DIGEST_SYN",      14,  P0, longTimeout,     GOSSIP,            () -> GossipDigestSyn.serializer,           () -> GossipDigestSynVerbHandler.instance                      );
    public static Verb GOSSIP_DIGEST_ACK      = new Verb("GOSSIP_DIGEST_ACK",      15,  P0, longTimeout,     GOSSIP,            () -> GossipDigestAck.serializer,           () -> GossipDigestAckVerbHandler.instance                      );
    public static Verb GOSSIP_DIGEST_ACK2     = new Verb("GOSSIP_DIGEST_ACK2",     16,  P0, longTimeout,     GOSSIP,            () -> GossipDigestAck2.serializer,          () -> GossipDigestAck2VerbHandler.instance                     );
    public static Verb GOSSIP_SHUTDOWN        = new Verb("GOSSIP_SHUTDOWN",        29,  P0, rpcTimeout,      GOSSIP,            () -> NoPayload.serializer,                 () -> GossipShutdownVerbHandler.instance                       );

    public static Verb ECHO_RSP               = new Verb("ECHO_RSP",               91,  P0, rpcTimeout,      GOSSIP,            () -> NoPayload.serializer,                 () -> ResponseVerbHandler.instance                             );
    public static Verb ECHO_REQ               = new Verb("ECHO_REQ",               31,  P0, rpcTimeout,      GOSSIP,            () -> NoPayload.serializer,                 () -> EchoVerbHandler.instance,            ECHO_RSP            );
    public static Verb PING_RSP               = new Verb("PING_RSP",               97,  P1, pingTimeout,     GOSSIP,            () -> NoPayload.serializer,                 () -> ResponseVerbHandler.instance                             );
    public static Verb PING_REQ               = new Verb("PING_REQ",               37,  P1, pingTimeout,     GOSSIP,            () -> PingRequest.serializer,               () -> PingVerbHandler.instance,            PING_RSP            );

    // public static Verb P1 because messages can be arbitrarily large or aren't crucial
    public static Verb SCHEMA_PUSH_RSP        = new Verb("SCHEMA_PUSH_RSP",        98,  P1, rpcTimeout,      MIGRATION,         () -> NoPayload.serializer,                 () -> ResponseVerbHandler.instance                             );
    public static Verb SCHEMA_PUSH_REQ        = new Verb("SCHEMA_PUSH_REQ",        18,  P1, rpcTimeout,      MIGRATION,         () -> SchemaMutationsSerializer.instance,   () -> SchemaPushVerbHandler.instance,      SCHEMA_PUSH_RSP     );
    public static Verb SCHEMA_PULL_RSP        = new Verb("SCHEMA_PULL_RSP",        88,  P1, rpcTimeout,      MIGRATION,         () -> SchemaMutationsSerializer.instance,   () -> ResponseVerbHandler.instance                             );
    public static Verb SCHEMA_PULL_REQ        = new Verb("SCHEMA_PULL_REQ",        28,  P1, rpcTimeout,      MIGRATION,         () -> NoPayload.serializer,                 () -> SchemaPullVerbHandler.instance,      SCHEMA_PULL_RSP     );
    public static Verb SCHEMA_VERSION_RSP     = new Verb("SCHEMA_VERSION_RSP",     80,  P1, rpcTimeout,      MIGRATION,         () -> UUIDSerializer.serializer,            () -> ResponseVerbHandler.instance                             );
    public static Verb SCHEMA_VERSION_REQ     = new Verb("SCHEMA_VERSION_REQ",     20,  P1, rpcTimeout,      MIGRATION,         () -> NoPayload.serializer,                 () -> SchemaVersionVerbHandler.instance,   SCHEMA_VERSION_RSP  );

    // repair; mostly doesn't use callbacks and sends responses as their own request messages, with matching sessions by uuid; should eventually harmonize and make idiomatic
    public static Verb REPAIR_RSP             = new Verb("REPAIR_RSP",             100, P1, repairMsgTimeout,REQUEST_RESPONSE,  () -> NoPayload.serializer,                 () -> ResponseVerbHandler.instance                             );
    public static Verb VALIDATION_RSP         = new Verb("VALIDATION_RSP",         102, P1, longTimeout     ,ANTI_ENTROPY,      () -> ValidationResponse.serializer,        () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          );
    public static Verb VALIDATION_REQ         = new Verb("VALIDATION_REQ",         101, P1, repairMsgTimeout,ANTI_ENTROPY,      () -> ValidationRequest.serializer,         () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          );
    public static Verb SYNC_RSP               = new Verb("SYNC_RSP",               104, P1, repairMsgTimeout,ANTI_ENTROPY,      () -> SyncResponse.serializer,              () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          );
    public static Verb SYNC_REQ               = new Verb("SYNC_REQ",               103, P1, repairMsgTimeout,ANTI_ENTROPY,      () -> SyncRequest.serializer,               () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          );
    public static Verb PREPARE_MSG            = new Verb("PREPARE_MSG",            105, P1, prepareTimeout,  ANTI_ENTROPY,      () -> PrepareMessage.serializer,            () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          );
    public static Verb SNAPSHOT_MSG           = new Verb("SNAPSHOT_MSG",           106, P1, repairMsgTimeout,ANTI_ENTROPY,      () -> SnapshotMessage.serializer,           () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          );
    public static Verb CLEANUP_MSG            = new Verb("CLEANUP_MSG",            107, P1, repairMsgTimeout,ANTI_ENTROPY,      () -> CleanupMessage.serializer,            () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          );
    public static Verb PREPARE_CONSISTENT_RSP = new Verb("PREPARE_CONSISTENT_RSP", 109, P1, repairMsgTimeout,ANTI_ENTROPY,      () -> PrepareConsistentResponse.serializer, () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          );
    public static Verb PREPARE_CONSISTENT_REQ = new Verb("PREPARE_CONSISTENT_REQ", 108, P1, repairMsgTimeout,ANTI_ENTROPY,      () -> PrepareConsistentRequest.serializer,  () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          );
    public static Verb FINALIZE_PROPOSE_MSG   = new Verb("FINALIZE_PROPOSE_MSG",   110, P1, repairMsgTimeout,ANTI_ENTROPY,      () -> FinalizePropose.serializer,           () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          );
    public static Verb FINALIZE_PROMISE_MSG   = new Verb("FINALIZE_PROMISE_MSG",   111, P1, repairMsgTimeout,ANTI_ENTROPY,      () -> FinalizePromise.serializer,           () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          );
    public static Verb FINALIZE_COMMIT_MSG    = new Verb("FINALIZE_COMMIT_MSG",    112, P1, repairMsgTimeout,ANTI_ENTROPY,      () -> FinalizeCommit.serializer,            () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          );
    public static Verb FAILED_SESSION_MSG     = new Verb("FAILED_SESSION_MSG",     113, P1, repairMsgTimeout,ANTI_ENTROPY,      () -> FailSession.serializer,               () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          );
    public static Verb STATUS_RSP             = new Verb("STATUS_RSP",             115, P1, repairMsgTimeout,ANTI_ENTROPY,      () -> StatusResponse.serializer,            () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          );
    public static Verb STATUS_REQ             = new Verb("STATUS_REQ",             114, P1, repairMsgTimeout,ANTI_ENTROPY,      () -> StatusRequest.serializer,             () -> RepairMessageVerbHandler.instance,   REPAIR_RSP          );

    public static Verb REPLICATION_DONE_RSP   = new Verb("REPLICATION_DONE_RSP",   82,  P0, rpcTimeout,      MISC,              () -> NoPayload.serializer,                 () -> ResponseVerbHandler.instance                             );
    public static Verb REPLICATION_DONE_REQ   = new Verb("REPLICATION_DONE_REQ",   22,  P0, rpcTimeout,      MISC,              () -> NoPayload.serializer,                 () -> ReplicationDoneVerbHandler.instance, REPLICATION_DONE_RSP);
    public static Verb SNAPSHOT_RSP           = new Verb("SNAPSHOT_RSP",           87,  P0, rpcTimeout,      MISC,              () -> NoPayload.serializer,                 () -> ResponseVerbHandler.instance                             );
    public static Verb SNAPSHOT_REQ           = new Verb("SNAPSHOT_REQ",           27,  P0, rpcTimeout,      MISC,              () -> SnapshotCommand.serializer,           () -> SnapshotVerbHandler.instance,        SNAPSHOT_RSP        );

    // generic failure response
    public static Verb FAILURE_RSP            = new Verb("FAILURE_RSP",            99,  P0, noTimeout,       REQUEST_RESPONSE,  () -> RequestFailureReason.serializer,      CustomResponseVerbHandlerProvider.instance                     );

    // dummy verbs
    public static Verb _TRACE                 = new Verb("_TRACE",                 30,  P1, rpcTimeout,      TRACING,           () -> NoPayload.serializer,                 () -> null                                                     );
    public static Verb _SAMPLE                = new Verb("_SAMPLE",                42,  P1, rpcTimeout,      INTERNAL_RESPONSE, () -> NoPayload.serializer,                 () -> null                                                     );
    public static Verb _TEST_1                = new Verb("_TEST_1",                10,  P0, writeTimeout,    IMMEDIATE,         () -> NoPayload.serializer,                 () -> null                                                     );
    public static Verb _TEST_2                = new Verb("_TEST_2",                11,  P1, rpcTimeout,      IMMEDIATE,         () -> NoPayload.serializer,                 () -> null                                                     );

    @Deprecated
    public static Verb REQUEST_RSP            = new Verb("REQUEST_RSP",            4,   P1, rpcTimeout,      REQUEST_RESPONSE,  () -> null,                                 CustomResponseVerbHandlerProvider.instance                     );
    @Deprecated
    public static Verb INTERNAL_RSP           = new Verb("INTERNAL_RSP",           23,  P1, rpcTimeout,      INTERNAL_RESPONSE, () -> null,                                 () -> ResponseVerbHandler.instance                             );

    // largest used public static Verb ID: 116

    // CUSTOM VERBS
    public static Verb UNUSED_CUSTOM_VERB     = new Verb("UNUSED_CUSTOM_VERB", CUSTOM,         0,   P1, rpcTimeout,      INTERNAL_RESPONSE, null,                                 () -> null                                                     );

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

    private final String name;
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
    private Supplier<? extends IVerbHandler<?>> handler;

    final Verb responseVerb;

    private final ToLongFunction<TimeUnit> expiration;

    /**
     * Verbs it's okay to drop if the request has been queued longer than the request timeout.  These
     * all correspond to client requests or something triggered by them; we don't want to
     * drop internal messages like bootstrap or repair notifications.
     */
    Verb(String name, int id, Priority priority, ToLongFunction<TimeUnit> expiration, Stage stage, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> serializer, Supplier<? extends IVerbHandler<?>> handler)
    {
        this(name, id, priority, expiration, stage, serializer, handler, null);
    }

    Verb(String name, int id, Priority priority, ToLongFunction<TimeUnit> expiration, Stage stage, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> serializer, Supplier<? extends IVerbHandler<?>> handler, Verb responseVerb)
    {
        this(name, NORMAL, id, priority, expiration, stage, serializer, handler, responseVerb);
    }

    Verb(String name, Kind kind, int id, Priority priority, ToLongFunction<TimeUnit> expiration, Stage stage, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> serializer, Supplier<? extends IVerbHandler<?>> handler)
    {
        this(name, kind, id, priority, expiration, stage, serializer, handler, null);
    }

    Verb(String name, Kind kind, int id, Priority priority, ToLongFunction<TimeUnit> expiration, Stage stage, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> serializer, Supplier<? extends IVerbHandler<?>> handler, Verb responseVerb)
    {
        this.stage = stage;
        if (id < 0)
            throw new IllegalArgumentException("Verb id must be non-negative, got " + id + " for verb " + name);

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
        this.name = name;
        this.priority = priority;
        this.serializer = serializer;
        this.handler = handler;
        this.responseVerb = responseVerb;
        this.expiration = expiration;
        this.kind = kind;

         verbs.add(this);
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
        return handler.get() == ResponseVerbHandler.instance;
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
        Field modifiers = Field.class.getDeclaredField("modifiers");
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
        Field modifiers = Field.class.getDeclaredField("modifiers");
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
        Field modifiers = Field.class.getDeclaredField("modifiers");
        modifiers.setAccessible(true);
        modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(this, expiration);
        return original;
    }

    @Override
    public String toString()
    {
        return name();
    }

    public String name()
    {
        return name;
    }

    // This is the largest number we can store in 2 bytes using VIntCoding (1 bit per byte is used to indicate if there is more data coming).
    // When generating ids we count *down* from this number
    private static final int CUSTOM_VERB_START = (1 << (7 * 2)) - 1;

    // Sanity check for the custom verb ids - avoids someone mistakenly adding a custom verb id close to the normal verbs which
    // could cause a conflict later when new normal verbs are added.
    private static final int MAX_CUSTOM_VERB_ID = 1000;

    private static final Verb[] idToVerbMap;
    private static volatile Verb[] idToCustomVerbMap;
    private static volatile int minCustomId;

    static
    {
        List<Verb> verbs = getValues();
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
                    assertCustomIdIsUnused(customIdMap, relativeId, v.name);
                    customIdMap[relativeId] = v;
                    break;
                default:
                    throw new AssertionError("Unsupported Verb Kind: " + v.kind + " for verb " + v);
            }
        }

        idToVerbMap = idMap;
        idToCustomVerbMap = customIdMap;
    }

    private static void assertCustomIdIsUnused(Verb[] customIdMap, int id, String name)
    {
        if (id < customIdMap.length && customIdMap[id] != null)
            throw new IllegalArgumentException("cannot have two custom verbs that map to the same id: " + name + " and " + customIdMap[id]);
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
     * Convert to/from relative and absolute id for a custom verb.
     *
     * <pre>{@code
     *          relId = idForCustomVerb(absId)
     *          absId = idForCustomVerb(relId).
     * }</pre>
     *
     * <p>Relative ids can be used for indexing idToCustomVerbMap. Absolute ids exist to distinguish
     * regular verbs from custom verbs in the id space.</p>
     *
     * @param id the relative or absolute id.
     * @return a relative id if {@code id} is absolute, or absolute id if {@code id} is relative.
     */
    private static int idForCustomVerb(int id)
    {
        return CUSTOM_VERB_START - id;
    }

    /**
     * Add a new custom verb to the list of verbs.
     *
     * <p>While we could dynamically generate an {@code id} for callers, it's safer to have users
     * explicitly control the id space since it prevents nodes with different versions disagreeing on which
     * verb has which id, e.g. during upgrade.</p>
     *
     * @param name the name of the new verb.
     * @param id the identifier for this custom verb (must be relative id and >= 0 && <= MAX_CUSTOM_VERB_ID).
     * @param priority the priority of the new verb.
     * @param expiration an optional timeout for this verb. @see VerbTimeouts.
     * @param stage The stage this verb should execute in.
     * @param serializer A method to serialize this verb
     * @param handler A method to handle this verb when received by the network
     * @param responseVerb The verb to respond with (optional)
     * @return A Verb for the newly added verb.
     */
    public static synchronized Verb addCustomVerb(String name, int id, Priority priority, ToLongFunction<TimeUnit> expiration, Stage stage, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> serializer, Supplier<? extends IVerbHandler<?>> handler, Verb responseVerb)
    {
        assertNameIsUnused(name);
        assertCustomIdIsUnused(idToCustomVerbMap, id, name);
        Verb verb = new Verb(name, CUSTOM, id, priority, expiration, stage, serializer, handler, responseVerb);

        int absoluteId = idForCustomVerb(id);
        minCustomId = Math.min(absoluteId, minCustomId);

        Verb[] newMap = Arrays.copyOf(idToCustomVerbMap, CUSTOM_VERB_START - minCustomId + 1);
        System.arraycopy(idToCustomVerbMap, 0, newMap, 0, idToCustomVerbMap.length);
        newMap[id] = verb;
        idToCustomVerbMap = newMap;

        return verb;
    }

    // Callers must take care of synchronizing to protect against concurrent updates to verbs.
    private static void assertNameIsUnused(String name)
    {
        if (verbs.stream().map(v -> v.name).collect(Collectors.toList()).contains(name))
            throw new IllegalArgumentException("Verb name '" + name + "' already exists");
    }

    /**
     * Decorates the specified verb handler with the provided method.
     *
     * <p>An example use case is to run a custom method after every write request.</p>
     *
     * @param verbs the list of verbs whose handlers should be wrapped by {@code decoratorFn}.
     * @param decoratorFn the method that decorates the handlers in verbs.
     */
    public static synchronized void decorateHandler(List<Verb> verbs, Function<IVerbHandler<?>, IVerbHandler<?>> decoratorFn)
    {
        for (Verb v : verbs)
        {
            IVerbHandler<?> handler = v.handler();
            final IVerbHandler<?> decoratedHandler = decoratorFn.apply(handler);
            v.handler = () -> decoratedHandler;
        }
    }
}

@SuppressWarnings("unused")
class VerbTimeouts
{
    static final ToLongFunction<TimeUnit> rpcTimeout      = DatabaseDescriptor::getRpcTimeout;
    static final ToLongFunction<TimeUnit> writeTimeout    = DatabaseDescriptor::getWriteRpcTimeout;
    static final ToLongFunction<TimeUnit> hintTimeout     = DatabaseDescriptor::getHintsRpcTimeout;
    static final ToLongFunction<TimeUnit> readTimeout     = DatabaseDescriptor::getReadRpcTimeout;
    static final ToLongFunction<TimeUnit> rangeTimeout    = DatabaseDescriptor::getRangeRpcTimeout;
    static final ToLongFunction<TimeUnit> counterTimeout  = DatabaseDescriptor::getCounterWriteRpcTimeout;
    static final ToLongFunction<TimeUnit> truncateTimeout = DatabaseDescriptor::getTruncateRpcTimeout;
    static final ToLongFunction<TimeUnit> pingTimeout     = DatabaseDescriptor::getPingTimeout;
    static final ToLongFunction<TimeUnit> longTimeout     = units -> Math.max(DatabaseDescriptor.getRpcTimeout(units), units.convert(5L, TimeUnit.MINUTES));
    static final ToLongFunction<TimeUnit> prepareTimeout  = DatabaseDescriptor::getRepairPrepareMessageTimeout;
    static final ToLongFunction<TimeUnit> noTimeout       = units -> { throw new IllegalStateException(); };
    static final ToLongFunction<TimeUnit> repairMsgTimeout= DatabaseDescriptor::getRepairRpcTimeout;
}
