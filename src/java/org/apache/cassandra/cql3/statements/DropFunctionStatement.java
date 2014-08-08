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

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.UFMetaData;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.transport.Event;

/**
 * A <code>DROP FUNCTION</code> statement parsed from a CQL query.
 */
public final class DropFunctionStatement extends SchemaAlteringStatement
{
    private final String namespace;
    private final String functionName;
    private final String qualifiedName;
    private final boolean ifExists;

    public DropFunctionStatement(String namespace, String functionName, boolean ifExists)
    {
        super();
        this.namespace = namespace == null ? "" : namespace;
        this.functionName = functionName;
        this.qualifiedName = UFMetaData.qualifiedName(namespace, functionName);
        this.ifExists = ifExists;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException
    {
        // TODO CASSANDRA-7557 (function DDL permission)

        state.hasAllKeyspacesAccess(Permission.DROP);
    }

    /**
     * The <code>CqlParser</code> only goes as far as extracting the keyword arguments
     * from these statements, so this method is responsible for processing and
     * validating.
     *
     * @throws org.apache.cassandra.exceptions.InvalidRequestException if arguments are missing or unacceptable
     */
    public void validate(ClientState state) throws RequestValidationException
    {
        if (!namespace.isEmpty() && !namespace.matches("\\w+"))
            throw new InvalidRequestException(String.format("\"%s\" is not a valid function name", qualifiedName));
        if (!functionName.matches("\\w+"))
            throw new InvalidRequestException(String.format("\"%s\" is not a valid function name", qualifiedName));
        if (namespace.length() > Schema.NAME_LENGTH)
            throw new InvalidRequestException(String.format("UDF namespace names shouldn't be more than %s characters long (got \"%s\")", Schema.NAME_LENGTH, qualifiedName));
        if (functionName.length() > Schema.NAME_LENGTH)
            throw new InvalidRequestException(String.format("UDF function names shouldn't be more than %s characters long (got \"%s\")", Schema.NAME_LENGTH, qualifiedName));
    }

    public Event.SchemaChange changeEvent()
    {
        return null;
    }

    // no execute() - drop propagated via MigrationManager

    public void announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        try
        {
            MigrationManager.announceFunctionDrop(namespace, functionName, isLocalOnly);
        }
        catch (InvalidRequestException e)
        {
            if (!ifExists)
                throw e;
        }
    }
}
