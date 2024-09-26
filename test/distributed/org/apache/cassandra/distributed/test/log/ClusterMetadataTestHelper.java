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

package org.apache.cassandra.distributed.test.log;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;


import org.apache.cassandra.ServerTestUtils.ResettableClusterMetadataService;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.CreateKeyspaceStatement;
import org.apache.cassandra.cql3.statements.schema.KeyspaceAttributes;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTransformation;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.AtomicLongBackedProcessor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Commit;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.TokenMap;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;
import org.apache.cassandra.tcm.sequences.BootstrapAndJoin;
import org.apache.cassandra.tcm.sequences.BootstrapAndReplace;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.sequences.Move;
import org.apache.cassandra.tcm.sequences.LeaveStreams;
import org.apache.cassandra.tcm.sequences.ReconfigureCMS;
import org.apache.cassandra.tcm.sequences.UnbootstrapAndLeave;
import org.apache.cassandra.tcm.transformations.AlterSchema;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.tcm.transformations.PrepareMove;
import org.apache.cassandra.tcm.transformations.PrepareReplace;
import org.apache.cassandra.tcm.transformations.Register;
import org.apache.cassandra.tcm.transformations.cms.AdvanceCMSReconfiguration;
import org.apache.cassandra.tcm.transformations.cms.PrepareCMSReconfiguration;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;

import static org.junit.Assert.assertEquals;

public class ClusterMetadataTestHelper
{
    public static final InetAddressAndPort node1;
    public static final InetAddressAndPort broadcastAddress;

    static
    {
        try
        {
            node1 = InetAddressAndPort.getByName("127.0.1.99");
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException("Error initializing InetAddressAndPort");
        }
        broadcastAddress = FBUtilities.getBroadcastAddressAndPort();
    }

    public static void setInstanceForTest()
    {
        ClusterMetadataService.setInstance(instanceForTest());
    }

    /**
     * Create a blank CMS which supports mark & reset for use in tests. This version takes care of initial
     * CMS setup by bootstrapping the log and applying an Initialize transformation.
     * @return a resettable CMS instance, to be used in a call to ClusterMetadataService::setInstance
     */
    public static ClusterMetadataService instanceForTest()
    {
        ClusterMetadata current = new ClusterMetadata(DatabaseDescriptor.getPartitioner());
        LocalLog log = LocalLog.logSpec()
                               .withInitialState(current)
                               .createLog();
        ResettableClusterMetadataService service = new ResettableClusterMetadataService(new UniformRangePlacement(),
                                                                                        MetadataSnapshots.NO_OP,
                                                                                        log,
                                                                                        new AtomicLongBackedProcessor(log),
                                                                                        Commit.Replicator.NO_OP,
                                                                                        true);
        log.readyUnchecked();
        log.bootstrap(FBUtilities.getBroadcastAddressAndPort());
        QueryProcessor.registerStatementInvalidatingListener();
        service.mark();
        return service;
    }

    public static ClusterMetadata minimalForTesting(Epoch epoch, IPartitioner partitioner)
    {
        return new ClusterMetadata(epoch, Murmur3Partitioner.instance,
                                   DistributedSchema.empty(),
                                   Directory.EMPTY,
                                   new TokenMap(partitioner),
                                   DataPlacements.empty(),
                                   LockedRanges.EMPTY,
                                   InProgressSequences.EMPTY,
                                   ImmutableMap.of());
    }

    public static ClusterMetadata minimalForTesting(IPartitioner partitioner)
    {
        return new ClusterMetadata(Epoch.EMPTY,
                                   partitioner,
                                   null,
                                   null,
                                   null,
                                   DataPlacements.empty(),
                                   null,
                                   null,
                                   ImmutableMap.of());
    }

    public static ClusterMetadata minimalForTesting(Keyspaces keyspaces)
    {
        return new ClusterMetadata(Epoch.EMPTY,
                                   Murmur3Partitioner.instance,
                                   new DistributedSchema(keyspaces),
                                   null,
                                   null,
                                   DataPlacements.empty(),
                                   null,
                                   null,
                                   ImmutableMap.of());
    }

    public static ClusterMetadataService syncInstanceForTest()
    {
        LocalLog log = LocalLog.logSpec()
                               .sync()
                               .createLog();
        ClusterMetadataService cms = new ClusterMetadataService(new UniformRangePlacement(),
                                                                MetadataSnapshots.NO_OP,
                                                                log,
                                                                new AtomicLongBackedProcessor(log),
                                                                Commit.Replicator.NO_OP,
                                                                true);

        log.readyUnchecked();
        return cms;
    }

    public static void createKeyspace(String name, KeyspaceParams params)
    {
        KeyspaceAttributes attributes = new KeyspaceAttributes();
        attributes.addProperty(KeyspaceParams.Option.REPLICATION.toString(), params.replication.asMap());
        CreateKeyspaceStatement createKeyspaceStatement = new CreateKeyspaceStatement(name, attributes, false);
        try
        {
            commit(new AlterSchema(createKeyspaceStatement, Schema.instance));
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void createKeyspace(String statement)
    {
        CreateKeyspaceStatement createKeyspaceStatement = (CreateKeyspaceStatement) QueryProcessor.parseStatement(statement).prepare(ClientState.forInternalCalls());
        try
        {
            commit(new AlterSchema(createKeyspaceStatement, Schema.instance));
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }

    private static Set<InetAddressAndPort> leaving(ClusterMetadata metadata)
    {
        return  metadata.directory.states.entrySet().stream()
                                         .filter(e -> e.getValue() == NodeState.LEAVING)
                                         .map(e -> metadata.directory.endpoint(e.getKey()))
                                         .collect(Collectors.toSet());
    }

    public static Map<Token, InetAddressAndPort> bootstrapping(ClusterMetadata metadata)
    {
        return  metadata.directory.states.entrySet().stream()
                                         .filter(e -> e.getValue() == NodeState.BOOTSTRAPPING)
                                         .collect(Collectors.toMap(e -> metadata.tokenMap.tokens(e.getKey()).iterator().next(),
                                                                   e -> metadata.directory.endpoint(e.getKey())));
    }

    public static NodeId register(int nodeIdx)
    {
        return register(nodeIdx, "dc0", "rack0");
    }

    public static NodeId register(InetAddressAndPort addr)
    {
        return register(addr, "dc0", "rack0");
    }

    public static NodeId nodeId(int nodeIdx)
    {
        return nodeId(addr(nodeIdx));
    }

    public static NodeId nodeId(InetAddressAndPort addr)
    {
        return ClusterMetadata.current().directory.peerId(addr);
    }

    public static InetAddressAndPort addr(int nodeIdx)
    {
        try
        {
            return InetAddressAndPort.getByName("127.0.0." + nodeIdx);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static NodeAddresses addr(InetAddressAndPort address)
    {
        return new NodeAddresses(address);
    }

    public static NodeId register(int nodeIdx, String dc, String rack)
    {
        return register(addr(nodeIdx), dc, rack);
    }

    public static NodeId register(InetAddressAndPort endpoint, String dc, String rack)
    {
        try
        {
            return commit(new Register(addr(endpoint), new Location(dc, rack), NodeVersion.CURRENT)).directory.peerId(endpoint);
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void join(int nodeIdx, long token)
    {
        join(addr(nodeIdx), Collections.singleton(new Murmur3Partitioner.LongToken(token)));
    }

    public static void join(InetAddressAndPort addr, Token token)
    {
        join(addr, Collections.singleton(token));
    }

    public static void join(InetAddressAndPort addr, Collection<Token> tokens)
    {
        try
        {
            NodeId nodeId = ClusterMetadata.current().directory.peerId(addr);
            JoinProcess process = lazyJoin(addr, tokens);
            process.prepareJoin()
                   .startJoin()
                   .midJoin()
                   .finishJoin();

            assert ClusterMetadata.current().inProgressSequences.get(nodeId) == null;
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void joinPartially(InetAddressAndPort addr, Token token)
    {
        joinPartially(addr, Collections.singleton(token));
    }

    public static void joinPartially(InetAddressAndPort addr, Set<Token> tokens)
    {
        try
        {
            NodeId nodeId = ClusterMetadata.current().directory.peerId(addr);
            JoinProcess process = lazyJoin(addr, tokens);
            process.prepareJoin()
                   .startJoin()
                   .midJoin();

            assert ClusterMetadata.current().inProgressSequences.get(nodeId) != null;
            assert ClusterMetadata.current().directory.peerState(nodeId) == NodeState.BOOTSTRAPPING;
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void movePartially(InetAddressAndPort addr, Set<Token> newTokens)
    {
        try
        {
            NodeId nodeId = ClusterMetadata.current().directory.peerId(addr);
            // for now we preserve the constraint that can only move when non-vnodes
            assertEquals(1, ClusterMetadata.current().tokenMap.tokens(nodeId).size());
            assertEquals(1, newTokens.size());
            MoveProcess process = lazyMove(addr, newTokens);
            process.prepareMove()
                   .startMove();

            assert ClusterMetadata.current().inProgressSequences.get(nodeId) != null;
            assert ClusterMetadata.current().directory.peerState(nodeId) == NodeState.MOVING;
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void leave(int nodeIdx)
    {
            leave(addr(nodeIdx));
    }

    public static void leave(InetAddressAndPort endpoint)
    {
        try
        {
            NodeId nodeId = nodeId(endpoint);
            LeaveProcess process = lazyLeave(endpoint, false);
            process.prepareLeave()
                   .startLeave()
                   .midLeave()
                   .finishLeave();

            assert  ClusterMetadata.current().inProgressSequences.get(nodeId) == null;
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }

    public static JoinProcess lazyJoin(int nodeIdx, long token)
    {
        return lazyJoin(addr(nodeIdx), Collections.singleton(new Murmur3Partitioner.LongToken(token)));
    }

    public static JoinProcess lazyJoin(InetAddressAndPort endpoint, Token token)
    {
        return lazyJoin(endpoint, Collections.singleton(token));
    }

    public static JoinProcess lazyJoin(InetAddressAndPort endpoint, Collection<Token> tokens)
    {
        return new JoinProcess()
        {
            int idx = 0;

            public JoinProcess prepareJoin()
            {
                assert idx == 0;
                try
                {
                    NodeId nodeId = ClusterMetadata.current().directory.peerId(endpoint);
                    commit(new PrepareJoin(nodeId,
                                           Sets.newHashSet(tokens),
                                           ClusterMetadataService.instance().placementProvider(),
                                           true,
                                           false));
                    idx++;
                    return this;
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            }

            public JoinProcess startJoin()
            {
                assert idx == 1;
                try
                {
                    BootstrapAndJoin plan = getBootstrapPlan(endpoint);
                    assert plan.next == Transformation.Kind.START_JOIN;
                    commit(plan.startJoin);
                    idx++;
                    return this;
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            }

            public JoinProcess midJoin()
            {
                assert idx == 2;
                try
                {
                    BootstrapAndJoin plan = getBootstrapPlan(endpoint);
                    assert plan.next == Transformation.Kind.MID_JOIN;
                    commit(plan.midJoin);
                    idx++;
                    return this;
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            }

            public JoinProcess finishJoin()
            {
                assert idx == 3;
                try
                {
                    BootstrapAndJoin plan = getBootstrapPlan(endpoint);
                    assert plan.next == Transformation.Kind.FINISH_JOIN;
                    commit(plan.finishJoin);
                    idx++;
                    return this;
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public static LeaveProcess lazyLeave(int nodeIdx)
    {
        try
        {
            return lazyLeave(InetAddressAndPort.getByName("127.0.0." + nodeIdx));
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static LeaveProcess lazyLeave(InetAddressAndPort endpoint)
    {
        return lazyLeave(endpoint, false);
    }

    public static LeaveProcess lazyLeave(int idx, boolean force)
    {
        return lazyLeave(addr(idx), force);
    }

    public static LeaveProcess lazyLeave(InetAddressAndPort endpoint, boolean force)
    {
        return new LeaveProcess()
        {
            int idx = 0;
            public LeaveProcess prepareLeave()
            {
                assert idx == 0;
                try
                {
                    NodeId nodeId = ClusterMetadata.current().directory.peerId(endpoint);
                    commit(new PrepareLeave(nodeId,
                                            force,
                                            ClusterMetadataService.instance().placementProvider(),
                                            LeaveStreams.Kind.UNBOOTSTRAP));
                    idx++;
                    return this;
                }
                catch (Throwable e)
                {
                    throw Throwables.throwAsUncheckedException(e);
                }
            }

            public LeaveProcess startLeave()
            {
                assert idx == 1;
                try
                {
                    UnbootstrapAndLeave plan = getLeavePlan(endpoint);
                    assert plan.next == Transformation.Kind.START_LEAVE;
                    commit(plan.startLeave);
                    idx++;
                    return this;
                }
                catch (Throwable e)
                {
                    throw Throwables.throwAsUncheckedException(e);
                }
            }

            public LeaveProcess midLeave()
            {
                assert idx == 2;
                try
                {
                    UnbootstrapAndLeave plan = getLeavePlan(endpoint);
                    assert plan.next == Transformation.Kind.MID_LEAVE;
                    commit(plan.midLeave);
                    idx++;
                    return this;
                }
                catch (Throwable e)
                {
                    throw Throwables.throwAsUncheckedException(e);
                }
            }

            public LeaveProcess finishLeave()
            {
                assert idx == 3;
                try
                {
                    UnbootstrapAndLeave plan = getLeavePlan(endpoint);
                    assert plan.next == Transformation.Kind.FINISH_LEAVE;
                    commit(plan.finishLeave);
                    idx++;
                    return this;
                }
                catch (Throwable e)
                {
                    throw Throwables.throwAsUncheckedException(e);
                }
            }
        };
    }

    public static void replace(int replaced, int replacement)
    {
        replace(addr(replaced), addr(replacement));
    }

    public static void replace(InetAddressAndPort replaced, InetAddressAndPort replacement)
    {
        try
        {
            NodeId replacementId = ClusterMetadata.current().directory.peerId(replacement);
            ReplaceProcess process = lazyReplace(replaced, replacement);
            process.prepareReplace()
                   .startReplace()
                   .midReplace()
                   .finishReplace();

            assert ClusterMetadata.current().inProgressSequences.get(replacementId) == null;
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }

    public static ReplaceProcess lazyReplace(int replaced, int replacement)
    {
        return lazyReplace(addr(replaced), addr(replacement));
    }

    public static ReplaceProcess lazyReplace(InetAddressAndPort replaced, InetAddressAndPort replacement)
    {
        return new ReplaceProcess()
        {
            int idx = 0;

            public ReplaceProcess prepareReplace()
            {
                assert idx == 0;
                try
                {
                    NodeId replacedId = ClusterMetadata.current().directory.peerId(replaced);
                    NodeId replacementId = ClusterMetadata.current().directory.peerId(replacement);
                    commit(new PrepareReplace(replacedId,
                                              replacementId,
                                              ClusterMetadataService.instance().placementProvider(),
                                              true,
                                              false));
                    idx++;
                    return this;
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            }

            public ReplaceProcess startReplace()
            {
                assert idx == 1;
                try
                {
                    BootstrapAndReplace plan = getReplacePlan(replacement);
                    assert plan.next == Transformation.Kind.START_REPLACE;
                    commit(plan.startReplace);
                    idx++;
                    return this;
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            }

            public ReplaceProcess midReplace()
            {
                assert idx == 2;
                try
                {
                    BootstrapAndReplace plan = getReplacePlan(replacement);
                    assert plan.next == Transformation.Kind.MID_REPLACE;
                    commit(plan.midReplace);
                    idx++;
                    return this;
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            }

            public ReplaceProcess finishReplace()
            {
                assert idx == 3;
                try
                {
                    BootstrapAndReplace plan = getReplacePlan(replacement);
                    assert plan.next == Transformation.Kind.FINISH_REPLACE;
                    commit(plan.finishReplace);
                    idx++;
                    return this;
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public static MoveProcess lazyMove(InetAddressAndPort endpoint, Set<Token> tokens)
    {
        return new MoveProcess()
        {
            int idx = 0;

            public MoveProcess prepareMove()
            {
                assert idx == 0;
                try
                {
                    NodeId id = ClusterMetadata.current().directory.peerId(endpoint);
                    commit(new PrepareMove(id,
                                           tokens,
                                           ClusterMetadataService.instance().placementProvider(),
                                           false));
                    idx++;
                    return this;
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            }

            public MoveProcess startMove()
            {
                assert idx == 1;
                try
                {
                    Move plan = getMovePlan(endpoint);
                    assert plan.next == Transformation.Kind.START_MOVE;
                    commit(plan.startMove);
                    idx++;
                    return this;
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            }

            public MoveProcess midMove()
            {
                assert idx == 2;
                try
                {
                    Move plan = getMovePlan(endpoint);
                    assert plan.next == Transformation.Kind.MID_MOVE;
                    commit(plan.midMove);
                    idx++;
                    return this;
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            }

            public MoveProcess finishMove()
            {
                assert idx == 3;
                try
                {
                    Move plan = getMovePlan(endpoint);
                    assert plan.next == Transformation.Kind.FINISH_MOVE;
                    commit(plan.finishMove);
                    idx++;
                    return this;
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public static void addEndpoint(int i)
    {
        try
        {
            addEndpoint(InetAddressAndPort.getByName("127.0.0." + i), new Murmur3Partitioner.LongToken(i));
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void addEndpoint(InetAddressAndPort endpoint, Token t)
    {
        addEndpoint(endpoint, t, "dc1", "rack1");
    }

    public static void addEndpoint(InetAddressAndPort endpoint, Collection<Token> tokens)
    {
        addEndpoint(endpoint, tokens, "dc1", "rack1");
    }

    public static void addEndpoint(InetAddressAndPort endpoint, Token t, Location location)
    {
        addEndpoint(endpoint, Collections.singleton(t), location.datacenter, location.rack);
    }

    public static void addEndpoint(InetAddressAndPort endpoint, Token t, String dc, String rack)
    {
        addEndpoint(endpoint, Collections.singleton(t), dc, rack);
    }

    public static void addEndpoint(InetAddressAndPort endpoint, Collection<Token> t, String dc, String rack)
    {
        try
        {
            Location l = new Location(dc, rack);
            commit(new Register(addr(endpoint), l, NodeVersion.CURRENT));
            lazyJoin(endpoint, new HashSet<>(t)).prepareJoin()
                                                .startJoin()
                                                .midJoin()
                                                .finishJoin();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void removeEndpoint(InetAddressAndPort endpoint, boolean force)
    {
        lazyLeave(endpoint, force)
        .prepareLeave()
        .startLeave()
        .midLeave()
        .finishLeave();
    }

    public static void reconfigureCms(ReplicationParams replication)
    {
        ClusterMetadata metadata = ClusterMetadataService.instance().commit(new PrepareCMSReconfiguration.Complex(replication, Collections.emptySet()));
        while (metadata.inProgressSequences.contains(ReconfigureCMS.SequenceKey.instance))
        {
            AdvanceCMSReconfiguration next = ((ReconfigureCMS) metadata.inProgressSequences.get(ReconfigureCMS.SequenceKey.instance)).next;
            metadata = ClusterMetadataService.instance().commit(next);
        }
    }
    public static void addOrUpdateKeyspace(KeyspaceMetadata keyspace)
    {
        try
        {
            SchemaTransformation transformation = (cm) -> cm.schema.getKeyspaces().withAddedOrUpdated(keyspace);
            commit(new AlterSchema(transformation, Schema.instance));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static ClusterMetadata commit(Transformation transform) throws ExecutionException, InterruptedException
    {
        return ClusterMetadataService.instance().commit(transform);
    }

    public static interface NodeOperations
    {
        public void register();
        public void join();
        public void leave();
        public JoinProcess lazyJoin();
        public LeaveProcess lazyLeave();
    }

    public static interface JoinProcess
    {
        public JoinProcess prepareJoin();
        public JoinProcess startJoin();
        public JoinProcess midJoin();
        public JoinProcess finishJoin();
    }

    public static interface LeaveProcess
    {
        public LeaveProcess prepareLeave();
        public LeaveProcess startLeave();
        public LeaveProcess midLeave();
        public LeaveProcess finishLeave();
    }

    public static interface ReplaceProcess
    {
        public ReplaceProcess prepareReplace();
        public ReplaceProcess startReplace();
        public ReplaceProcess midReplace();
        public ReplaceProcess finishReplace();
    }

    public static interface MoveProcess
    {
        public MoveProcess prepareMove();
        public MoveProcess startMove();
        public MoveProcess midMove();
        public MoveProcess finishMove();
    }

    public static VersionedEndpoints.ForToken getNaturalReplicasForToken(String keyspace, Token searchPosition)
    {
        return getNaturalReplicasForToken(ClusterMetadata.current(), keyspace, searchPosition);
    }

    public static PrepareJoin prepareJoin(int idx)
    {
        return prepareJoin(nodeId(idx));
    }
    public static PrepareJoin prepareJoin(NodeId nodeId)
    {
        return new PrepareJoin(nodeId,
                               Collections.singleton(Murmur3Partitioner.instance.getRandomToken()),
                               new UniformRangePlacement(),
                               true,
                               false);
    }

    public static PrepareReplace prepareReplace(int replaced, int replacement)
    {
        return prepareReplace(nodeId(replaced), nodeId(replacement));
    }

    public static PrepareReplace prepareReplace(NodeId replaced, NodeId replacement)
    {
        return new PrepareReplace(replaced,
                                  replacement,
                                  new UniformRangePlacement(),
                                  true,
                                  false);
    }

    public static PrepareLeave prepareLeave(int idx)
    {
        return prepareLeave(nodeId(idx));
    }
    public static PrepareLeave prepareLeave(NodeId nodeId)
    {
        return new PrepareLeave(nodeId,
                                false,
                                new UniformRangePlacement(),
                                LeaveStreams.Kind.UNBOOTSTRAP);
    }

    public static PrepareMove prepareMove(NodeId id, Token newToken)
    {
        return new PrepareMove(id,
                               Collections.singleton(Murmur3Partitioner.instance.getRandomToken()),
                               new UniformRangePlacement(),
                               false);
    }

    /**
     * get the (possibly cached) endpoints that should store the given Token.
     * Note that while the endpoints are conceptually a Set (no duplicates will be included),
     * we return a List to avoid an extra allocation when sorting by proximity later
     *
     * @param searchPosition the position the natural endpoints are requested for
     * @return a copy of the natural endpoints for the given token
     */
    public static VersionedEndpoints.ForToken getNaturalReplicasForToken(ClusterMetadata metadata, String keyspace, Token searchPosition)
    {
        KeyspaceMetadata keyspaceMetadata = metadata.schema.getKeyspaces().getNullable(keyspace);
        return metadata.placements.get(keyspaceMetadata.params.replication).reads.forToken(searchPosition);
    }

    public static BootstrapAndJoin getBootstrapPlan(int idx)
    {
        return getBootstrapPlan(addr(idx));
    }

    public static BootstrapAndJoin getBootstrapPlan(InetAddressAndPort addr)
    {
        return getBootstrapPlan(ClusterMetadata.current().directory.peerId(addr));
    }

    public static BootstrapAndJoin getBootstrapPlan(NodeId nodeId)
    {
        return (BootstrapAndJoin) ClusterMetadata.current().inProgressSequences.get(nodeId);
    }

    public static BootstrapAndJoin getBootstrapPlan(NodeId nodeId, ClusterMetadata metadata)
    {
        return (BootstrapAndJoin) metadata.inProgressSequences.get(nodeId);
    }

    public static UnbootstrapAndLeave getLeavePlan(int peer)
    {
        return getLeavePlan(addr(peer));
    }

    public static UnbootstrapAndLeave getLeavePlan(InetAddressAndPort addr)
    {
        return getLeavePlan(ClusterMetadata.current().directory.peerId(addr));
    }

    public static UnbootstrapAndLeave getLeavePlan(NodeId nodeId)
    {
        return (UnbootstrapAndLeave) ClusterMetadata.current().inProgressSequences.get(nodeId);
    }

    public static BootstrapAndReplace getReplacePlan(int idx)
    {
        return getReplacePlan(addr(idx));
    }

    public static BootstrapAndReplace getReplacePlan(InetAddressAndPort addr)
    {
        return getReplacePlan(ClusterMetadata.current().directory.peerId(addr));
    }

    public static BootstrapAndReplace getReplacePlan(NodeId nodeId)
    {
        return (BootstrapAndReplace) ClusterMetadata.current().inProgressSequences.get(nodeId);
    }

    public static BootstrapAndReplace getReplacePlan(NodeId nodeId, ClusterMetadata metadata)
    {
        return (BootstrapAndReplace) metadata.inProgressSequences.get(nodeId);
    }

    public static Move getMovePlan(InetAddressAndPort addr)
    {
        return getMovePlan(ClusterMetadata.current().directory.peerId(addr));
    }

    public static Move getMovePlan(NodeId nodeId)
    {
        return (Move) ClusterMetadata.current().inProgressSequences.get(nodeId);
    }

    public static Move getMovePlan(NodeId nodeId, ClusterMetadata metadata)
    {
        return (Move) metadata.inProgressSequences.get(nodeId);
    }

    public static Token bytesToken(int token)
    {
        return new ByteOrderedPartitioner.BytesToken(ByteBufferUtil.bytes(token));
    }

    private static final Random random = new Random();
    public static int randomInt()
    {
        return randomInt(Integer.MAX_VALUE);
    }

    public static int randomInt(int max)
    {
        return random.nextInt(max);
    }

    public static ListenableFuture<MessageDelivery> registerOutgoingMessageSink(Verb... ignored)
    {
        final SettableFuture<MessageDelivery> future = SettableFuture.create();
        Set<Verb> ignore = Sets.newHashSet(ignored);
        MessagingService.instance().outboundSink.clear();
        MessagingService.instance().outboundSink.add((Message<?> message, InetAddressAndPort to) ->
                                                     {
                                                         if (!ignore.contains(message.verb()))
                                                             future.set(new MessageDelivery(message, to));
                                                         return true;
                                                     });

        MessagingService.instance().inboundSink.clear();
        MessagingService.instance().inboundSink.add((Message<?> message) -> false);

        return future;
    }

    public static class MessageDelivery
    {
        public final Message<?> message;
        public final InetAddressAndPort to;

        MessageDelivery(Message<?> message, InetAddressAndPort to)
        {
            this.message = message;
            this.to = to;
        }
    }
}
