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

package org.apache.cassandra.cql3.statements;

import java.util.function.UnaryOperator;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.service.ClientState;

/**
 * A super class for raw (parsed) statements which supports keyspace override during preparation.
 *
 * @param <R> a type of the statement produced by preparation of this raw statement
 */
public abstract class RawKeyspaceAwareStatement<R extends CQLStatement> extends CQLStatement.Raw
{
    /**
     * Produces a prepared statement of type {@link #<R>} without overriding keyspace.
     */
    @Override
    public final R prepare(ClientState state)
    {
        return prepare(state, Constants.IDENTITY_STRING_MAPPER);
    }

    /**
     * Produces a prepared statement of type {@link #<R>}, optionally overriding keyspace name in the produced
     * statement. The keyspace name is overridden using the provided mapping function in the statement and all
     * contained objects which refer to some keyspace.
     */
    public abstract R prepare(ClientState state, UnaryOperator<String> keyspaceMapper);
}
