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

package org.apache.cassandra.simulator.test;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Invariants;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.harry.op.Visit;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.execution.CompiledStatement;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.execution.QueryBuildingVisitExecutor;
import org.apache.cassandra.harry.execution.ResultSetRow;
import org.apache.cassandra.harry.model.TokenPlacementModel;
import org.apache.cassandra.harry.util.ByteUtils;
import org.apache.cassandra.harry.util.TokenUtil;
import org.apache.cassandra.simulator.systems.InterceptedExecution;
import org.apache.cassandra.simulator.systems.InterceptingExecutor;
import org.apache.cassandra.simulator.systems.SimulatedAction;

public class HarryValidatingQuery extends SimulatedAction
{
    private static final Logger logger = LoggerFactory.getLogger(HarryValidatingQuery.class);

    private final InterceptingExecutor on;
    private final Cluster cluster;
    private final List<TokenPlacementModel.Node> owernship;
    private final TokenPlacementModel.ReplicationFactor rf;

    private final HarrySimulatorTest.HarrySimulation simulation;
    private final Visit visit;
    private final QueryBuildingVisitExecutor queryBuilder;

    public HarryValidatingQuery(HarrySimulatorTest.HarrySimulation simulation,
                                Cluster cluster,
                                TokenPlacementModel.ReplicationFactor rf,
                                List<TokenPlacementModel.Node> owernship,
                                Visit visit,
                                QueryBuildingVisitExecutor queryBuilder)
    {
        super(visit, Modifiers.RELIABLE_NO_TIMEOUTS, Modifiers.RELIABLE_NO_TIMEOUTS, null, simulation.simulated);
        this.rf = rf;
        this.cluster = cluster;
        this.on = (InterceptingExecutor) cluster.get(1).executor();
        this.owernship = owernship;
        this.visit = visit;
        this.queryBuilder = queryBuilder;
        this.simulation = simulation;

    }

    protected InterceptedExecution task()
    {
        return new InterceptedExecution.InterceptedTaskExecution(on)
        {
            public void run()
            {
                try
                {
                    TokenPlacementModel.ReplicatedRanges ring = rf.replicate(owernship);
                    Invariants.checkState(visit.operations.length == 1);
                    Invariants.checkState(visit.operations[0] instanceof Operations.SelectStatement);
                    Operations.SelectStatement select = (Operations.SelectStatement) visit.operations[0];
                    for (TokenPlacementModel.Replica replica : ring.replicasFor(token(select.pd)))
                    {
                        CompiledStatement compiled = queryBuilder.compile(visit);
                        Object[][] objects = executeNodeLocal(compiled.cql(), replica.node(), compiled.bindings());
                        List<ResultSetRow> actualRows = InJvmDTestVisitExecutor.rowsToResultSet(simulation.schema, select, objects);
                        simulation.model.validate(select, actualRows);
                    }
                }
                catch (Throwable t)
                {
                    logger.error("Caught an exception while validating", t);
                    throw t;
                }
            }
        };
    }

    protected long token(long pd)
    {
        return TokenUtil.token(ByteUtils.compose(ByteUtils.objectsToBytes(simulation.schema.valueGenerators.pkGen.inflate(pd))));
    }

    protected Object[][] executeNodeLocal(String statement, TokenPlacementModel.Node node, Object... bindings)
    {
        IInstance instance = cluster
                             .stream()
                             .filter((n) -> n.config().broadcastAddress().toString().equals(node.id()))
                             .findFirst()
                             .get();
        return instance.executeInternal(statement, bindings);
    }
}
