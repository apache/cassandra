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

package org.apache.cassandra.harry.sut;

import java.util.concurrent.CompletableFuture;

import org.apache.cassandra.harry.operations.CompiledStatement;

public interface SystemUnderTest
{
    public interface SUTFactory
    {
        public SystemUnderTest make();
    }

    public boolean isShutdown();

    public void shutdown();

    default void afterSchemaInit()
    {
    }

    default void schemaChange(String statement)
    {
        execute(statement, ConsistencyLevel.ALL, new Object[]{});
    }

    Object[][] execute(String statement, ConsistencyLevel cl, Object... bindings);

    default Object[][] execute(CompiledStatement statement, ConsistencyLevel cl)
    {
        return execute(statement.cql(), cl, statement.bindings());
    }

    default Object[][] executeIdempotent(String statement, ConsistencyLevel cl, Object... bindings)
    {
        return execute(statement, cl, bindings);
    }

    CompletableFuture<Object[][]> executeAsync(String statement, ConsistencyLevel cl, Object... bindings);

    default CompletableFuture<Object[][]> executeIdempotentAsync(String statement, ConsistencyLevel cl, Object... bindings)
    {
        return executeAsync(statement, cl, bindings);
    }

    interface SystemUnderTestFactory
    {
        SystemUnderTest create();
    }

    enum ConsistencyLevel {
        ALL, QUORUM, NODE_LOCAL, ONE
    }

    public static final SystemUnderTest NO_OP = new NoOpSut();

    public class NoOpSut implements SystemUnderTest
    {
        private NoOpSut() {}
        public boolean isShutdown()
        {
            return false;
        }

        public void shutdown()
        {
        }

        public Object[][] execute(String statement, ConsistencyLevel cl,  Object... bindings)
        {
            return new Object[0][];
        }

        public CompletableFuture<Object[][]> executeAsync(String statement, ConsistencyLevel cl, Object... bindings)
        {
            return CompletableFuture.supplyAsync(() -> execute(statement, cl, bindings),
                                                 Runnable::run);
        }
    }

    public static interface FaultInjectingSut extends SystemUnderTest
    {
        public Object[][] executeWithWriteFailure(String statement, ConsistencyLevel cl, Object... bindings);
        public CompletableFuture<Object[][]> executeAsyncWithWriteFailure(String statement, ConsistencyLevel cl, Object... bindings);
    }

}