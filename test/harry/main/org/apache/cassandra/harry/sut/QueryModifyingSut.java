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

import org.apache.cassandra.harry.model.AgainstSutChecker;

/**
 * Best thing for sanity checking of tricky range tombstone issues:
 * write to memtable instead of writing to the usual system under test.
 *
 * Usually used in conjunction with {@link AgainstSutChecker}:
 *
 *             SchemaSpec doubleWriteSchema = schema.cloneWithName(schema.keyspace, schema.keyspace + "_debug");
 *
 *             sut.schemaChange(doubleWriteSchema.compile().cql());
 *
 *             QueryModifyingSut sut = new QueryModifyingSut(this.sut,
 *                                                           schema.table,
 *                                                           doubleWriteSchema.table);
 *
 *
 *             Model model = new AgainstSutChecker(tracker, history.clock(), sut, schema, doubleWriteSchema);
 */
public class QueryModifyingSut implements SystemUnderTest
{
    private final SystemUnderTest delegate;
    private final String toReplace;
    private final String replacement;

    public QueryModifyingSut(SystemUnderTest delegate,
                             String toReplace,
                             String replacement)
    {
        this.delegate = delegate;
        this.toReplace = toReplace;
        this.replacement = replacement;
    }

    public boolean isShutdown()
    {
        return delegate.isShutdown();
    }

    public void shutdown()
    {
        delegate.shutdown();
    }

    public Object[][] execute(String statement, ConsistencyLevel cl, Object... bindings)
    {
        return delegate.execute(statement.replaceAll(toReplace, replacement),
                                cl,
                                bindings);
    }

    public CompletableFuture<Object[][]> executeAsync(String statement, ConsistencyLevel cl, Object... bindings)
    {
        return delegate.executeAsync(statement.replaceAll(toReplace, replacement),
                                     cl,
                                     bindings);
    }
}
