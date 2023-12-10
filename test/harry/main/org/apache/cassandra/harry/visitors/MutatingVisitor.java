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

package org.apache.cassandra.harry.visitors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.operations.CompiledStatement;
import org.apache.cassandra.harry.tracker.DataTracker;

public class MutatingVisitor extends GeneratingVisitor
{
    private static final Logger logger = LoggerFactory.getLogger(MutatingVisitor.class);

    public MutatingVisitor(Run run)
    {
        this(run, MutatingRowVisitor::new);
    }

    public MutatingVisitor(Run run,
                           OperationExecutor.RowVisitorFactory rowVisitorFactory)
    {
        this(run, new MutatingVisitExecutor(run, rowVisitorFactory.make(run)));
    }

    public static Configuration.VisitorConfiguration factory()
    {
        return MutatingVisitor::new;
    }

    public static Configuration.VisitorConfiguration factory(Function<Run, VisitExecutor> rowVisitorFactory)
    {
        return (r) -> new MutatingVisitor(r, rowVisitorFactory.apply(r));
    }

    public MutatingVisitor(Run run,
                           VisitExecutor visitExecutor)
    {
        super(run, visitExecutor);
    }

    public static class LtsTrackingVisitExecutor extends MutatingVisitExecutor
    {
        public LtsTrackingVisitExecutor(OpSelectors.DescriptorSelector descriptorSelector, DataTracker tracker, SystemUnderTest sut, SchemaSpec schema, OperationExecutor rowVisitor, SystemUnderTest.ConsistencyLevel consistencyLevel)
        {
            super(descriptorSelector, tracker, sut, schema, rowVisitor, consistencyLevel);
            assert schema.trackLts : "LTS Tracking visitor can only be used when tracking LTS in schema";
        }

        @Override
        public void afterLts(long lts, long pd)
        {
            if (hadVisibleVisit)
            {
                StringBuilder sb = new StringBuilder();
                sb.append("UPDATE ").append(schema.keyspace).append(".").append(schema.table)
                  .append(" SET visited_lts = visited_lts + [").append(lts).append("] ")
                  .append("WHERE ");
                Object[] pk = schema.inflatePartitionKey(pd);
                for (int i = 0; i < pk.length; i++)
                {
                    if (i > 0)
                        sb.append(" AND ");
                    sb.append(schema.partitionKeys.get(i).name + " = ?");
                    bindings.add(pk[i]);
                }
                sb.append(";");
                statements.add(sb.toString());
            }

            super.afterLts(lts, pd);
        }
    }

    public static class MutatingVisitExecutor extends VisitExecutor
    {
        protected final List<String> statements = new ArrayList<>();
        protected final List<Object> bindings = new ArrayList<>();

        protected final OpSelectors.DescriptorSelector descriptorSelector;
        protected final DataTracker tracker;
        protected final SystemUnderTest sut;
        protected final OperationExecutor rowVisitor;
        protected final SystemUnderTest.ConsistencyLevel consistencyLevel;
        protected final SchemaSpec schema;
        private final int maxRetries = 10;
        // Apart from partition deletion, we register all operations on partition level
        protected Boolean hadVisibleVisit = null;

        public MutatingVisitExecutor(Run run, OperationExecutor rowVisitor)
        {
            this(run, rowVisitor, SystemUnderTest.ConsistencyLevel.QUORUM);
        }

        public MutatingVisitExecutor(Run run, OperationExecutor rowVisitor, SystemUnderTest.ConsistencyLevel consistencyLevel)
        {
            this(run.descriptorSelector, run.tracker, run.sut, run.schemaSpec, rowVisitor, consistencyLevel);
        }

        public MutatingVisitExecutor(OpSelectors.DescriptorSelector descriptorSelector,
                                     DataTracker tracker,
                                     SystemUnderTest sut,
                                     SchemaSpec schema,
                                     OperationExecutor rowVisitor,
                                     SystemUnderTest.ConsistencyLevel consistencyLevel)
        {
            this.descriptorSelector = descriptorSelector;
            this.tracker = tracker;
            this.sut = sut;
            this.schema = schema;
            this.rowVisitor = rowVisitor;
            this.consistencyLevel = consistencyLevel;
        }

        @Override
        public void beforeLts(long lts, long pd)
        {
            tracker.beginModification(lts);
        }

        @Override
        public void afterLts(long lts, long pd)
        {
            if (statements.isEmpty())
            {
                logger.warn("Encountered an empty batch on {}", lts);
                return;
            }

            String query = String.join(" ", statements);
            if (statements.size() > 1)
                query = String.format("BEGIN UNLOGGED BATCH\n%s\nAPPLY BATCH;", query);

            Object[] bindingsArray = new Object[bindings.size()];
            bindings.toArray(bindingsArray);
            statements.clear();
            bindings.clear();

            CompiledStatement compiledStatement = new CompiledStatement(query, bindingsArray);
            executeWithRetries(lts, pd, compiledStatement);
            tracker.endModification(lts);
            hadVisibleVisit = null;
        }

        @Override
        public void operation(Operation operation)
        {
            // Partition deletions have highest precedence even in batches, so we have to make
            // a distinction between "we have not seen any operations yet" and "there was a partition deletion"
            if (hadVisibleVisit == null)
                hadVisibleVisit = operation.kind().hasVisibleVisit();
            else
                hadVisibleVisit &= operation.kind().hasVisibleVisit();

            operationInternal(operation, rowVisitor.perform(operation));
        }

        protected void operationInternal(Operation operation, CompiledStatement statement)
        {
            statements.add(statement.cql());
            Collections.addAll(bindings, statement.bindings());
        }

        protected Object[][] executeWithRetries(long lts, long pd, CompiledStatement statement)
        {
            if (sut.isShutdown())
                throw new IllegalStateException("System under test is shut down");

            int retries = 0;

            while (retries++ < maxRetries)
            {
                try
                {
                    return sut.execute(statement.cql(), consistencyLevel, statement.bindings());
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

        public void shutdown() throws InterruptedException
        {
        }
    }
}
