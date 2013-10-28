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
package org.apache.cassandra.cql.hooks;

import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.base.Optional;

import org.apache.cassandra.thrift.ThriftClientState;

/**
 * Contextual information about the execution of a CQLStatement.
 * Used by {@link org.apache.cassandra.cql.hooks.PreExecutionHook}
 *
 * The CQL string representing the statement being executed is optional
 * and is not present for prepared statements. Contexts created for the
 * execution of regular (i.e. non-prepared) statements will always
 * contain a CQL string.
 */
public class ExecutionContext
{
    public final ThriftClientState clientState;
    public final Optional<String> queryString;
    public final List<ByteBuffer> variables;

    public ExecutionContext(ThriftClientState clientState, String queryString, List<ByteBuffer> variables)
    {
        this.clientState = clientState;
        this.queryString = Optional.fromNullable(queryString);
        this.variables = variables;
    }
}
