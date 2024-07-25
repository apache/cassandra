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

package org.apache.cassandra.simulator.harry;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.data.ResultSetRow;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.model.SelectHelper;
import org.apache.cassandra.harry.operations.CompiledStatement;
import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.harry.sut.injvm.QuiescentLocalStateChecker;
import org.apache.cassandra.simulator.systems.InterceptedExecution;
import org.apache.cassandra.simulator.systems.InterceptingExecutor;
import org.apache.cassandra.simulator.systems.SimulatedAction;
import org.apache.cassandra.simulator.systems.SimulatedSystems;

public class HarryValidatingQuery extends SimulatedAction
{
    private static final Logger logger = LoggerFactory.getLogger(HarryValidatingQuery.class);
    private final Run run;
    private final Query query;
    private final InterceptingExecutor on;
    private final Cluster cluster;
    private final List<TokenPlacementModel.Node> owernship;
    private final TokenPlacementModel.ReplicationFactor rf;

    public HarryValidatingQuery(SimulatedSystems simulated,
                                Cluster cluster,
                                TokenPlacementModel.ReplicationFactor rf,
                                Run run,
                                List<TokenPlacementModel.Node> owernship,
                                Query query)
    {
        super(query, Modifiers.RELIABLE_NO_TIMEOUTS, Modifiers.RELIABLE_NO_TIMEOUTS, null, simulated);
        this.rf = rf;
        this.cluster = cluster;
        this.on =  (InterceptingExecutor) cluster.get(1).executor();
        this.run = run;
        this.query = query;
        this.owernship = owernship;
    }

    protected InterceptedExecution task()
    {
        return new InterceptedExecution.InterceptedTaskExecution(on)
        {
            public void run()
            {
                try
                {
                    QuiescentLocalStateChecker model = new QuiescentLocalStateChecker(run, rf)
                    {
                        @Override
                        protected TokenPlacementModel.ReplicatedRanges getRing()
                        {
                            return rf.replicate(owernship);
                        }

                        @Override
                        protected void validate(Query query, TokenPlacementModel.ReplicatedRanges ring)
                        {
                            CompiledStatement compiled = query.toSelectStatement();
                            List<TokenPlacementModel.Replica> replicas = ring.replicasFor(this.token(query.pd));
                            logger.trace("Predicted {} as replicas for {}. Ring: {}", new Object[]{ replicas, query.pd, ring });
                            List<Throwable> throwables = new ArrayList<>();
                            for (TokenPlacementModel.Replica replica : replicas)
                            {
                                try
                                {
                                    validate(() -> {
                                        Object[][] objects = this.executeNodeLocal(compiled.cql(), replica.node(), compiled.bindings());
                                        List<ResultSetRow> result = new ArrayList();
                                        int length = objects.length;
                                        for (int i = 0; i < length; ++i)
                                        {
                                            Object[] res = objects[i];
                                            result.add(SelectHelper.resultSetToRow(query.schemaSpec, this.clock, res));
                                        }

                                        return result;
                                    }, query);
                                }
                                catch (Model.ValidationException t)
                                {
                                    throwables.add(new AssertionError(String.format("Caught an exception while validating %s on %s", query, replica), t));
                                }
                            }

                            if (!throwables.isEmpty())
                            {
                                AssertionError error = new AssertionError(String.format("Could not validate %d out of %d replicas %s", throwables.size(), replicas.size(), replicas));
                                throwables.forEach(error::addSuppressed);
                                throw error;
                            }
                        }


                        @Override
                        protected Object[][] executeNodeLocal(String statement, TokenPlacementModel.Node node, Object... bindings)
                        {
                            IInstance instance = cluster
                                                 .stream()
                                                 .filter((n) -> n.config().broadcastAddress().toString().equals(node.id()))
                                                 .findFirst()
                                                 .get();
                            return instance.executeInternal(statement, bindings);
                        }
                    };

                    model.validate(query);

                }
                catch (Throwable t)
                {
                    logger.error("Caught an exception while validating", t);
                    throw t;
                }
            }
        };
    }
}
