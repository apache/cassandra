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

package org.apache.cassandra.harry.sut.injvm;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.harry.operations.CompiledStatement;
import org.apache.cassandra.harry.util.ByteUtils;
import org.apache.cassandra.harry.util.TokenUtil;
import org.apache.cassandra.harry.visitors.GeneratingVisitor;
import org.apache.cassandra.harry.visitors.LoggingVisitor;
import org.apache.cassandra.harry.visitors.OperationExecutor;
import org.apache.cassandra.harry.visitors.VisitExecutor;
import org.apache.cassandra.harry.visitors.Visitor;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstance;

import static org.apache.cassandra.harry.sut.TokenPlacementModel.peerStateToNodes;

public class InJVMTokenAwareVisitExecutor extends LoggingVisitor.LoggingVisitorExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(InJVMTokenAwareVisitExecutor.class);

    private final InJvmSut sut;
    private final TokenPlacementModel.ReplicationFactor rf;
    private final SystemUnderTest.ConsistencyLevel cl;
    private final SchemaSpec schema;
    private final int MAX_RETRIES = 10;

    public static Function<Run, VisitExecutor> factory(OperationExecutor.RowVisitorFactory rowVisitorFactory,
                                                       SystemUnderTest.ConsistencyLevel cl,
                                                       TokenPlacementModel.ReplicationFactor rf)
    {
        return (run) -> new InJVMTokenAwareVisitExecutor(run, rowVisitorFactory, cl, rf);
    }

    public InJVMTokenAwareVisitExecutor(Run run,
                                        OperationExecutor.RowVisitorFactory rowVisitorFactory,
                                        SystemUnderTest.ConsistencyLevel cl,
                                        TokenPlacementModel.ReplicationFactor rf)
    {
        super(run, rowVisitorFactory.make(run));
        this.sut = (InJvmSut) run.sut;
        this.schema = run.schemaSpec;
        this.cl = cl;
        this.rf = rf;
    }

    @Override
    protected Object[][] executeWithRetries(long lts, long pd, CompiledStatement statement)
    {
        if (sut.isShutdown())
            throw new IllegalStateException("System under test is shut down");

        int retries = 0;

        Object[] pk = schema.inflatePartitionKey(pd);
        List<TokenPlacementModel.Replica> replicas = getRing().replicasFor(TokenUtil.token(ByteUtils.compose(ByteUtils.objectsToBytes(pk))));
        while (retries++ < MAX_RETRIES)
        {
            try
            {
                TokenPlacementModel.Replica replica = replicas.get((int) (lts % replicas.size()));
                if (cl == SystemUnderTest.ConsistencyLevel.NODE_LOCAL)
                {
                    return executeNodeLocal(statement.cql(), replica.node(), statement.bindings());
                }
                else
                {
                    return sut.cluster
                           .stream()
                           .filter((n) -> n.config().broadcastAddress().toString().contains(replica.node().id()))
                           .findFirst()
                           .get()
                           .coordinator()
                           .execute(statement.cql(), InJvmSut.toApiCl(cl), statement.bindings());
                }
            }
            catch (Throwable t)
            {
                int delaySecs = 1;
                logger.error(String.format("Caught message while trying to execute: %s. Scheduled to retry in %s seconds", statement, delaySecs), t);
                Uninterruptibles.sleepUninterruptibly(delaySecs, TimeUnit.SECONDS);
            }
        }
        throw new IllegalStateException(String.format("Can not execute statement %s after %d retries", statement, retries));
    }

    protected TokenPlacementModel.ReplicatedRanges getRing()
    {
        ICoordinator coordinator = sut.firstAlive().coordinator();
        List<TokenPlacementModel.Node> other = peerStateToNodes(coordinator.execute("select peer, tokens, data_center, rack from system.peers", ConsistencyLevel.ONE));
        List<TokenPlacementModel.Node> self = peerStateToNodes(coordinator.execute("select broadcast_address, tokens, data_center, rack from system.local", ConsistencyLevel.ONE));
        List<TokenPlacementModel.Node> all = new ArrayList<>();
        all.addAll(self);
        all.addAll(other);
        all.sort(TokenPlacementModel.Node::compareTo);
        return rf.replicate(all);
    }

    protected Object[][] executeNodeLocal(String statement, TokenPlacementModel.Node node, Object... bindings)
    {
        IInstance instance = sut.cluster
                             .stream()
                             .filter((n) -> n.config().broadcastAddress().toString().contains(node.id()))
                             .findFirst()
                             .get();
        return instance.executeInternal(statement, bindings);
    }


    @JsonTypeName("in_jvm_token_aware")
    public static class Configuration implements org.apache.cassandra.harry.core.Configuration.VisitorConfiguration
    {
        public final org.apache.cassandra.harry.core.Configuration.RowVisitorConfiguration row_visitor;
        public final SystemUnderTest.ConsistencyLevel consistency_level;
        public final int rf;
        @JsonCreator
        public Configuration(@JsonProperty("row_visitor") org.apache.cassandra.harry.core.Configuration.RowVisitorConfiguration rowVisitor,
                             @JsonProperty("consistency_level") SystemUnderTest.ConsistencyLevel consistencyLevel,
                             @JsonProperty("rf") int rf)
        {
            this.row_visitor = rowVisitor;
            this.consistency_level = consistencyLevel;
            this.rf = rf;
        }

        @Override
        public Visitor make(Run run)
        {
            return new GeneratingVisitor(run, new InJVMTokenAwareVisitExecutor(run, row_visitor, consistency_level, new TokenPlacementModel.SimpleReplicationFactor(rf)));
        }
    }
}