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

package org.apache.cassandra.distributed.fuzz;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import harry.core.Run;
import harry.ddl.SchemaSpec;
import harry.model.sut.SystemUnderTest;
import harry.operations.CompiledStatement;
import harry.visitors.*;
import org.apache.cassandra.distributed.test.log.RngUtils;

public class InJVMTokenAwareVisitorExecutor extends LoggingVisitor.LoggingVisitorExecutor
{
    public static void init()
    {
        harry.core.Configuration.registerSubtypes(Configuration.class);
    }

    private final InJvmSut sut;
    private final SystemUnderTest.ConsistencyLevel cl;
    private final SchemaSpec schema;
    private final int maxRetries = 10;

    // TODO: as of now, token aware executor only performs operations against natrual, but not pending replicas.
    public InJVMTokenAwareVisitorExecutor(Run run,
                                          OperationExecutor.RowVisitorFactory rowVisitorFactory,
                                          SystemUnderTest.ConsistencyLevel cl)
    {
        super(run, rowVisitorFactory.make(run));
        this.sut = (InJvmSut) run.sut;
        this.schema = run.schemaSpec;
        this.cl = cl;
    }

    @Override
    protected void executeAsyncWithRetries(long lts, long pd, CompletableFuture<Object[][]> future, CompiledStatement statement)
    {
        executeAsyncWithRetries(lts, pd, future, statement, 0);
    }

    private void executeAsyncWithRetries(long lts, long pd, CompletableFuture<Object[][]> future, CompiledStatement statement, int retries)
    {
        if (sut.isShutdown())
            throw new IllegalStateException("System under test is shut down");

        if (retries > this.maxRetries)
            throw new IllegalStateException(String.format("Can not execute statement %s after %d retries", statement, retries));

        Object[] partitionKey =  schema.inflatePartitionKey(pd);
        int[] replicas = sut.getReadReplicasFor(partitionKey, schema.keyspace, schema.table);
        int replica = replicas[RngUtils.asInt(lts, 0, replicas.length - 1)];
        if (cl == SystemUnderTest.ConsistencyLevel.NODE_LOCAL)
        {
            future.complete(sut.cluster.get(replica).executeInternal(statement.cql(), statement.bindings()));
        }
        else
        {
            CompletableFuture.supplyAsync(() -> sut.cluster.coordinator(replica).execute(statement.cql(), InJvmSut.toApiCl(cl), statement.bindings()), executor)
                             .whenComplete((res, t) ->
                                           {
                                               if (t != null)
                                                   executor.schedule(() -> executeAsyncWithRetries(lts, pd, future, statement, retries + 1), 1, TimeUnit.SECONDS);
                                               else
                                                   future.complete(res);
                                           });
        }
    }

    @JsonTypeName("in_jvm_token_aware")
    public static class Configuration implements harry.core.Configuration.VisitorConfiguration
    {
        public final harry.core.Configuration.RowVisitorConfiguration row_visitor;
        public final SystemUnderTest.ConsistencyLevel consistency_level;

        @JsonCreator
        public Configuration(@JsonProperty("row_visitor") harry.core.Configuration.RowVisitorConfiguration rowVisitor,
                             @JsonProperty("consistency_level") SystemUnderTest.ConsistencyLevel consistencyLevel)
        {
            this.row_visitor = rowVisitor;
            this.consistency_level = consistencyLevel;
        }

        @Override
        public Visitor make(Run run)
        {
            return new GeneratingVisitor(run, new InJVMTokenAwareVisitorExecutor(run, row_visitor, consistency_level));
        }
    }
}