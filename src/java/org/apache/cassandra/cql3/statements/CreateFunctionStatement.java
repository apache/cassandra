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
import java.util.HashSet;
import java.util.List;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.schema.Functions;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.Event;

/**
 * A {@code CREATE FUNCTION} statement parsed from a CQL query.
 */
public final class CreateFunctionStatement extends SchemaAlteringStatement
{
    private final boolean orReplace;
    private final boolean ifNotExists;
    private FunctionName functionName;
    private final String language;
    private final String body;

    private final List<ColumnIdentifier> argNames;
    private final List<CQL3Type.Raw> argRawTypes;
    private final CQL3Type.Raw rawReturnType;
    private final boolean calledOnNullInput;

    private List<AbstractType<?>> argTypes;
    private AbstractType<?> returnType;

    public CreateFunctionStatement(FunctionName functionName,
                                   String language,
                                   String body,
                                   List<ColumnIdentifier> argNames,
                                   List<CQL3Type.Raw> argRawTypes,
                                   CQL3Type.Raw rawReturnType,
                                   boolean calledOnNullInput,
                                   boolean orReplace,
                                   boolean ifNotExists)
    {
        this.functionName = functionName;
        this.language = language;
        this.body = body;
        this.argNames = argNames;
        this.argRawTypes = argRawTypes;
        this.rawReturnType = rawReturnType;
        this.calledOnNullInput = calledOnNullInput;
        this.orReplace = orReplace;
        this.ifNotExists = ifNotExists;
    }

    public Prepared prepare() throws InvalidRequestException
    {
        if (new HashSet<>(argNames).size() != argNames.size())
            throw new InvalidRequestException(String.format("duplicate argument names for given function %s with argument names %s",
                                                            functionName, argNames));

        argTypes = new ArrayList<>(argRawTypes.size());
        for (CQL3Type.Raw rawType : argRawTypes)
            argTypes.add(prepareType("arguments", rawType));

        returnType = prepareType("return type", rawReturnType);
        return super.prepare();
    }

    public void prepareKeyspace(ClientState state) throws InvalidRequestException
    {
        if (!functionName.hasKeyspace() && state.getRawKeyspace() != null)
            functionName = new FunctionName(state.getRawKeyspace(), functionName.name);

        if (!functionName.hasKeyspace())
            throw new InvalidRequestException("Functions must be fully qualified with a keyspace name if a keyspace is not set for the session");

        ThriftValidation.validateKeyspaceNotSystem(functionName.keyspace);
    }

    protected void grantPermissionsToCreator(QueryState state)
    {
        try
        {
            IResource resource = FunctionResource.function(functionName.keyspace, functionName.name, argTypes);
            DatabaseDescriptor.getAuthorizer().grant(AuthenticatedUser.SYSTEM_USER,
                                                     resource.applicablePermissions(),
                                                     resource,
                                                     RoleResource.role(state.getClientState().getUser().getName()));
        }
        catch (RequestExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        if (Schema.instance.findFunction(functionName, argTypes).isPresent() && orReplace)
            state.ensureHasPermission(Permission.ALTER, FunctionResource.function(functionName.keyspace,
                                                                                  functionName.name,
                                                                                  argTypes));
        else
            state.ensureHasPermission(Permission.CREATE, FunctionResource.keyspace(functionName.keyspace));
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        UDFunction.assertUdfsEnabled(language);

        if (ifNotExists && orReplace)
            throw new InvalidRequestException("Cannot use both 'OR REPLACE' and 'IF NOT EXISTS' directives");

        if (Schema.instance.getKSMetaData(functionName.keyspace) == null)
            throw new InvalidRequestException(String.format("Cannot add function '%s' to non existing keyspace '%s'.", functionName.name, functionName.keyspace));
    }

    public Event.SchemaChange announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        Function old = Schema.instance.findFunction(functionName, argTypes).orElse(null);
        boolean replaced = old != null;
        if (replaced)
        {
            if (ifNotExists)
                return null;
            if (!orReplace)
                throw new InvalidRequestException(String.format("Function %s already exists", old));
            if (!(old instanceof ScalarFunction))
                throw new InvalidRequestException(String.format("Function %s can only replace a function", old));
            if (calledOnNullInput != ((ScalarFunction) old).isCalledOnNullInput())
                throw new InvalidRequestException(String.format("Function %s can only be replaced with %s", old,
                                                                calledOnNullInput ? "CALLED ON NULL INPUT" : "RETURNS NULL ON NULL INPUT"));

            if (!Functions.typesMatch(old.returnType(), returnType))
                throw new InvalidRequestException(String.format("Cannot replace function %s, the new return type %s is not compatible with the return type %s of existing function",
                                                                functionName, returnType.asCQL3Type(), old.returnType().asCQL3Type()));
        }

        UDFunction udFunction = UDFunction.create(functionName, argNames, argTypes, returnType, calledOnNullInput, language, body);

        MigrationManager.announceNewFunction(udFunction, isLocalOnly);

        return new Event.SchemaChange(replaced ? Event.SchemaChange.Change.UPDATED : Event.SchemaChange.Change.CREATED,
                                      Event.SchemaChange.Target.FUNCTION,
                                      udFunction.name().keyspace, udFunction.name().name, AbstractType.asCQLTypeStringList(udFunction.argTypes()));
    }

    private AbstractType<?> prepareType(String typeName, CQL3Type.Raw rawType)
    {
        if (rawType.isFrozen())
            throw new InvalidRequestException(String.format("The function %s should not be frozen; remove the frozen<> modifier", typeName));

        // UDT are not supported non frozen but we do not allow the frozen keyword for argument. So for the moment we
        // freeze them here
        if (!rawType.canBeNonFrozen())
            rawType.freeze();

        AbstractType<?> type = rawType.prepare(functionName.keyspace).getType();
        return type;
    }
}
