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

import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

/**
 * Abstract class for statements that alter the schema.
 */
public abstract class SchemaAlteringStatement extends CFStatement implements CQLStatement
{
    private final boolean isColumnFamilyLevel;

    protected SchemaAlteringStatement()
    {
        super(null);
        this.isColumnFamilyLevel = false;
    }

    protected SchemaAlteringStatement(CFName name)
    {
        super(name);
        this.isColumnFamilyLevel = true;
    }

    public int getBoundTerms()
    {
        return 0;
    }

    @Override
    public void prepareKeyspace(ClientState state) throws InvalidRequestException
    {
        if (isColumnFamilyLevel)
            super.prepareKeyspace(state);
    }

    @Override
    public Prepared prepare()
    {
        return new Prepared(this);
    }

    public abstract ResultMessage.SchemaChange.Change changeType();

    public abstract void announceMigration() throws RequestValidationException;

    public ResultMessage execute(QueryState state, QueryOptions options) throws RequestValidationException
    {
        announceMigration();
        String tableName = cfName == null || columnFamily() == null ? "" : columnFamily();
        return new ResultMessage.SchemaChange(changeType(), keyspace(), tableName);
    }

    public ResultMessage executeInternal(QueryState state, QueryOptions options)
    {
        // executeInternal is for local query only, thus altering schema is not supported
        throw new UnsupportedOperationException();
    }
}
