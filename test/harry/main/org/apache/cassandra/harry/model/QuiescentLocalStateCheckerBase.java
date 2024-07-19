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

package org.apache.cassandra.harry.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.harry.sut.TokenPlacementModel.Replica;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.data.ResultSetRow;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.model.reconciler.Reconciler;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.harry.operations.CompiledStatement;
import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.harry.tracker.DataTracker;

import static org.apache.cassandra.harry.model.SelectHelper.resultSetToRow;
import static org.apache.cassandra.harry.sut.TokenPlacementModel.Node;

public abstract class QuiescentLocalStateCheckerBase extends QuiescentChecker
{
    private static final Logger logger = LoggerFactory.getLogger(QuiescentLocalStateCheckerBase.class);

    public final SystemUnderTest sut;
    public final TokenPlacementModel.ReplicationFactor rf;
    private final OpSelectors.PdSelector pdSelector;

    public QuiescentLocalStateCheckerBase(Run run, TokenPlacementModel.ReplicationFactor rf)
    {
        this(run.clock,
             run.pdSelector,
             run.sut,
             run.tracker,
             run.schemaSpec,
             new Reconciler(run),
             rf);
    }

    public QuiescentLocalStateCheckerBase(OpSelectors.Clock clock,
                                          OpSelectors.PdSelector pdSelector,
                                          SystemUnderTest sut,
                                          DataTracker tracker,
                                          SchemaSpec schema,
                                          Reconciler reconciler,
                                          TokenPlacementModel.ReplicationFactor rf)
    {
        super(clock, sut, tracker, schema, reconciler);
        this.sut = sut;
        this.rf = rf;
        this.pdSelector = pdSelector;
    }

    @SuppressWarnings("unused")
    public void validateAll()
    {
        TokenPlacementModel.ReplicatedRanges ring = getRing();

        for (int lts = 0; lts < clock.peek(); lts++)
            validate(Query.selectAllColumns(schema, pdSelector.pd(lts, schema), false), ring);
    }

    @Override
    public void validate(Query query)
    {
        TokenPlacementModel.ReplicatedRanges ring = getRing();
        tracker.beginValidation(query.pd);
        validate(query, ring);
        tracker.endValidation(query.pd);
    }

    protected void validate(Query query, TokenPlacementModel.ReplicatedRanges ring)
    {
        CompiledStatement compiled = query.toSelectStatement();
        List<Replica> replicas = ring.replicasFor(token(query.pd));

        logger.trace("Predicted {} as replicas for {}. Ring: {}", replicas, query.pd, ring);
        for (Replica replica : replicas)
        {
            try
            {
                validate(() -> {
                    Object[][] objects = executeNodeLocal(compiled.cql(), replica.node(), compiled.bindings());

                    List<ResultSetRow> result = new ArrayList<>();
                    for (Object[] obj : objects)
                        result.add(resultSetToRow(query.schemaSpec, clock, obj));

                    return result;
                }, query);
            }
            catch (ValidationException e)
            {
                throw new AssertionError(String.format("Caught error while validating replica %s of replica set %s",
                                                       replica, replicas),
                                         e);
            }
        }
    }

    protected abstract TokenPlacementModel.ReplicatedRanges getRing();
    protected abstract long token(long pd);
    protected abstract Object[][] executeNodeLocal(String statement, Node node, Object... bindings);
}
