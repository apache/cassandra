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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.operations.CompiledStatement;

/**
 * Fault injecting visitor: randomly fails some of the queries.
 *
 * Requires {@code FaultInjectingSut} to function.
 */
public class FaultInjectingVisitor extends LoggingVisitor
{
    private final AtomicInteger cnt = new AtomicInteger();

    private final SystemUnderTest.FaultInjectingSut sut;
    protected final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);

    public FaultInjectingVisitor(Run run, OperationExecutor.RowVisitorFactory rowVisitorFactory)
    {
        super(run, rowVisitorFactory);
        this.sut = (SystemUnderTest.FaultInjectingSut) run.sut;
    }

    void executeAsyncWithRetries(CompletableFuture<Object[][]> originator, CompiledStatement statement)
    {
        executeAsyncWithRetries(originator, statement, true);
    }

    void executeAsyncWithRetries(CompletableFuture<Object[][]> originator, CompiledStatement statement, boolean allowFailures)
    {
        if (sut.isShutdown())
            throw new IllegalStateException("System under test is shut down");

        CompletableFuture<Object[][]> future;
        if (allowFailures && cnt.getAndIncrement() % 2 == 0)
        {
            future = sut.executeAsyncWithWriteFailure(statement.cql(), SystemUnderTest.ConsistencyLevel.QUORUM, statement.bindings());
        }
        else
        {
            future = sut.executeAsync(statement.cql(), SystemUnderTest.ConsistencyLevel.QUORUM, statement.bindings());
        }

        future.whenComplete((res, t) -> {
               if (t != null)
                   executor.schedule(() -> executeAsyncWithRetries(originator, statement, false), 1, TimeUnit.SECONDS);
               else
                   originator.complete(res);
           });
    }

    @JsonTypeName("fault_injecting")
    public static class FaultInjectingVisitorConfiguration extends Configuration.MutatingVisitorConfiguation
    {
        @JsonCreator
        public FaultInjectingVisitorConfiguration(@JsonProperty("row_visitor") Configuration.RowVisitorConfiguration row_visitor)
        {
            super(row_visitor);
        }

        @Override
        public Visitor make(Run run)
        {
            return new FaultInjectingVisitor(run, row_visitor);
        }
    }

}
