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

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.UFMetaData;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Functions;
import org.apache.cassandra.cql3.udf.UDFRegistry;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.messages.ResultMessage;

/**
 * A <code>CREATE FUNCTION</code> statement parsed from a CQL query.
 */
public final class CreateFunctionStatement extends SchemaAlteringStatement
{
    final boolean orReplace;
    final boolean ifNotExists;
    final String namespace;
    final String functionName;
    final String qualifiedName;
    final String language;
    final String body;
    final boolean deterministic;
    final CQL3Type.Raw returnType;
    final List<Argument> arguments;

    private UFMetaData ufMeta;

    public CreateFunctionStatement(String namespace, String functionName, String language, String body, boolean deterministic,
                                   CQL3Type.Raw returnType, List<Argument> arguments, boolean orReplace, boolean ifNotExists)
    {
        super();
        this.namespace = namespace != null ? namespace : "";
        this.functionName = functionName;
        this.qualifiedName = UFMetaData.qualifiedName(namespace, functionName);
        this.language = language;
        this.body = body;
        this.deterministic = deterministic;
        this.returnType = returnType;
        this.arguments = arguments;
        assert functionName != null : "null function name";
        assert language != null : "null function language";
        assert body != null : "null function body";
        assert returnType != null : "null function returnType";
        assert arguments != null : "null function arguments";
        this.orReplace = orReplace;
        this.ifNotExists = ifNotExists;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException
    {
        // TODO CASSANDRA-7557 (function DDL permission)

        state.hasAllKeyspacesAccess(Permission.CREATE);
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

    public ResultMessage executeInternal(QueryState state, QueryOptions options)
    {
        try
        {
            doExecute();
            return super.executeInternal(state, options);
        }
        catch (RequestValidationException e)
        {
            throw new RuntimeException(e);
        }
    }

    public ResultMessage execute(QueryState state, QueryOptions options) throws RequestValidationException
    {
        doExecute();
        return super.execute(state, options);
    }

    private void doExecute() throws RequestValidationException
    {
        boolean exists = UDFRegistry.hasFunction(qualifiedName);
        if (exists && ifNotExists)
            throw new InvalidRequestException(String.format("Function '%s' already exists.", qualifiedName));
        if (exists && !orReplace)
            throw new InvalidRequestException(String.format("Function '%s' already exists.", qualifiedName));

        if (namespace.isEmpty() && Functions.contains(functionName))
            throw new InvalidRequestException(String.format("Function name '%s' is reserved by CQL.", qualifiedName));

        List<Argument> args = arguments;
        List<String> argumentNames = new ArrayList<>(args.size());
        List<String> argumentTypes = new ArrayList<>(args.size());
        for (Argument arg : args)
        {
            argumentNames.add(arg.getName().toString());
            argumentTypes.add(arg.getType().toString());
        }
        this.ufMeta = new UFMetaData(namespace, functionName, deterministic, argumentNames, argumentTypes,
                                     returnType.toString(), language, body);

        UDFRegistry.tryCreateFunction(ufMeta);
    }

    public boolean announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        MigrationManager.announceNewFunction(ufMeta, isLocalOnly);
        return true;
    }

    public static final class Argument
    {
        final ColumnIdentifier name;
        final CQL3Type.Raw type;

        public Argument(ColumnIdentifier name, CQL3Type.Raw type)
        {
            this.name = name;
            this.type = type;
        }

        public ColumnIdentifier getName()
        {
            return name;
        }

        public CQL3Type.Raw getType()
        {
            return type;
        }
    }
}
