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

import java.io.IOException;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.ThriftValidation;

public class DropKeyspaceStatement extends SchemaAlteringStatement
{
    private final String keyspace;

    public DropKeyspaceStatement(String keyspace)
    {
        super();
        this.keyspace = keyspace;
    }

    @Override
    public void validate(ClientState state) throws InvalidRequestException, SchemaDisagreementException
    {
        super.validate(state);
        ThriftValidation.validateKeyspaceNotSystem(keyspace);
    }

    public void announceMigration() throws ConfigurationException
    {
        MigrationManager.announceKeyspaceDrop(keyspace);
    }
}
