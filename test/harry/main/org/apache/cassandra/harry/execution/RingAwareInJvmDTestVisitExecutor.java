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

package org.apache.cassandra.harry.execution;


import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Invariants;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.op.Visit;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.model.QuiescentChecker;
import org.apache.cassandra.harry.model.TokenPlacementModel;
import org.apache.cassandra.harry.util.ByteUtils;
import org.apache.cassandra.harry.util.TokenUtil;

/**
 * Executes all modifications queries with NODE_LOCAL. Executes all validation queries
 * with NODE_LOCAL _on each node_.
 */
public class RingAwareInJvmDTestVisitExecutor extends InJvmDTestVisitExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(InJvmDTestVisitExecutor.class);

    private final TokenPlacementModel.ReplicationFactor rf;

    private RingAwareInJvmDTestVisitExecutor(SchemaSpec schema,
                                             DataTracker dataTracker,
                                             Model model,
                                             ICluster<?> cluster,
                                             NodeSelector nodeSelector,
                                             PageSizeSelector pageSizeSelector,
                                             RetryPolicy retryPolicy,
                                             ConsistencyLevelSelector consistencyLevel,
                                             TokenPlacementModel.ReplicationFactor rf,
                                             QueryBuildingVisitExecutor.WrapQueries wrapQueries)
    {
        super(schema, dataTracker, model, cluster, nodeSelector, pageSizeSelector, retryPolicy, consistencyLevel, QueryBuildingVisitExecutor.WrapQueries.UNLOGGED_BATCH);
        this.rf = rf;
    }

    protected TokenPlacementModel.ReplicatedRanges getRing()
    {
        IInstance node = ((Cluster)cluster).firstAlive();
        ICoordinator coordinator = node.coordinator();
        List<TokenPlacementModel.Node> other = TokenPlacementModel.peerStateToNodes(coordinator.execute("select peer, tokens, data_center, rack from system.peers", ConsistencyLevel.ONE));
        List<TokenPlacementModel.Node> self = TokenPlacementModel.peerStateToNodes(coordinator.execute("select broadcast_address, tokens, data_center, rack from system.local", ConsistencyLevel.ONE));
        List<TokenPlacementModel.Node> all = new ArrayList<>();
        all.addAll(self);
        all.addAll(other);
        all.sort(TokenPlacementModel.Node::compareTo);
        return rf.replicate(all);
    }

    public List<TokenPlacementModel.Replica> getReplicasFor(long pd)
    {
        return getRing().replicasFor(token(pd));
    }

    protected long token(long pd)
    {
        return TokenUtil.token(ByteUtils.compose(ByteUtils.objectsToBytes(schema.valueGenerators.pkGen().inflate(pd))));
    }

    @Override
    protected void executeValidatingVisit(Visit visit, List<Operations.SelectStatement> selects, CompiledStatement statement)
    {
        try
        {
            for (TokenPlacementModel.Replica replica : getReplicasFor(selects.get(0).pd))
            {
                IInstance instance = cluster
                                     .stream()
                                     .filter((n) -> n.config().broadcastAddress().toString().contains(replica.node().id()))
                                     .findFirst()
                                     .get();
                ConsistencyLevel consistencyLevel = this.consistencyLevel.consistencyLevel(visit);
                int pageSize = PageSizeSelector.NO_PAGING;
                if (consistencyLevel != ConsistencyLevel.NODE_LOCAL)
                    pageSize = pageSizeSelector.pages(visit);
                List<ResultSetRow> resultSetRow = executeWithResult(visit, instance.config().num(), pageSize, statement, consistencyLevel);
                model.validate(selects.get(0), resultSetRow);
            }
        }
        catch (Throwable t)
        {
            throw new AssertionError(String.format("Caught an exception while validating %s:\n%s", selects.get(0), statement), t);
        }
    }

    @Override
    protected void executeWithoutResult(Visit visit, CompiledStatement statement)
    {
        try
        {
            Invariants.checkState(visit.visitedPartitions.size() == 1,
                                  "Ring aware executor can only read and write one partition at a time");
            for (TokenPlacementModel.Replica replica : getReplicasFor(visit.visitedPartitions.iterator().next().longValue()))
            {
                IInstance instance = cluster
                                     .stream()
                                     .filter((n) -> n.config().broadcastAddress().toString().contains(replica.node().id()))
                                     .findFirst()
                                     .get();
                executeWithoutResult(visit, instance.config().num(), statement, consistencyLevel.consistencyLevel(visit));
            }
        }
        catch (Throwable t)
        {
            throw new AssertionError(String.format("Caught an exception while validating %s", statement), t);
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder extends InJvmDTestVisitExecutor.Builder
    {
        protected TokenPlacementModel.ReplicationFactor rf;

        public Builder replicationFactor(TokenPlacementModel.ReplicationFactor rf)
        {
            this.rf = rf;
            return this;
        }

        @Override
        public Builder wrapQueries(QueryBuildingVisitExecutor.WrapQueries wrapQueries)
        {
            super.wrapQueries(wrapQueries);
            return this;
        }

        @Override
        public Builder consistencyLevel(ConsistencyLevel consistencyLevel)
        {
            super.consistencyLevel(consistencyLevel);
            return this;
        }

        @Override
        public Builder nodeSelector(NodeSelector nodeSelector)
        {
            super.nodeSelector(nodeSelector);
            return this;
        }

        @Override
        public Builder pageSizeSelector(PageSizeSelector pageSizeSelector)
        {
            super.pageSizeSelector(pageSizeSelector);
            return this;
        }

        @Override
        public Builder retryPolicy(RetryPolicy retryPolicy)
        {
            super.retryPolicy(retryPolicy);
            return this;
        }

        @Override
        protected void setDefaults(SchemaSpec schema, ICluster<?> cluster)
        {
            super.setDefaults(schema, cluster);
            if (this.rf == null)
            {
                this.rf = new TokenPlacementModel.SimpleReplicationFactor(Math.min(3, cluster.size()));
            }
        }

        public RingAwareInJvmDTestVisitExecutor build(SchemaSpec schema, Model.Replay replay, ICluster<?> cluster)
        {
            DataTracker tracker = new DataTracker.SequentialDataTracker();
            Model model = new QuiescentChecker(schema.valueGenerators, tracker, replay);
            return build(schema, tracker, model, cluster);
        }

        public RingAwareInJvmDTestVisitExecutor build(SchemaSpec schema, DataTracker tracker, Model model, ICluster<?> cluster)
        {
            setDefaults(schema, cluster);
            return new RingAwareInJvmDTestVisitExecutor(schema, tracker, model, cluster,
                                                        nodeSelector, pageSizeSelector, retryPolicy, consistencyLevel, rf, wrapQueries);
        }
    }
}