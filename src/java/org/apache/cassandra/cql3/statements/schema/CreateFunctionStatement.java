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
package org.apache.cassandra.cql3.statements.schema;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.*;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.cql3.functions.UserFunction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.UserFunctions.FunctionsDiff;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

import static java.util.stream.Collectors.toList;

public final class CreateFunctionStatement extends AlterSchemaStatement
{
    private final String functionName;
    private final List<ColumnIdentifier> argumentNames;
    private final List<CQL3Type.Raw> rawArgumentTypes;
    private final CQL3Type.Raw rawReturnType;
    private final boolean calledOnNullInput;
    private final String language;
    private final String body;
    private final boolean orReplace;
    private final boolean ifNotExists;

    public CreateFunctionStatement(String keyspaceName,
                                   String functionName,
                                   List<ColumnIdentifier> argumentNames,
                                   List<CQL3Type.Raw> rawArgumentTypes,
                                   CQL3Type.Raw rawReturnType,
                                   boolean calledOnNullInput,
                                   String language,
                                   String body,
                                   boolean orReplace,
                                   boolean ifNotExists)
    {
        super(keyspaceName);
        this.functionName = functionName;
        this.argumentNames = argumentNames;
        this.rawArgumentTypes = rawArgumentTypes;
        this.rawReturnType = rawReturnType;
        this.calledOnNullInput = calledOnNullInput;
        this.language = language;
        this.body = body;
        this.orReplace = orReplace;
        this.ifNotExists = ifNotExists;
    }

    // TODO: replace affected aggregates !!
    public Keyspaces apply(Keyspaces schema)
    {
        if (ifNotExists && orReplace)
            throw ire("Cannot use both 'OR REPLACE' and 'IF NOT EXISTS' directives");

        UDFunction.assertUdfsEnabled(language);

        if (!FunctionName.isNameValid(functionName))
            throw ire("Function name '%s' is invalid", functionName);

        if (new HashSet<>(argumentNames).size() != argumentNames.size())
            throw ire("Duplicate argument names for given function %s with argument names %s", functionName, argumentNames);

        rawArgumentTypes.stream()
                        .filter(raw -> !raw.isImplicitlyFrozen() && raw.isFrozen())
                        .findFirst()
                        .ifPresent(t -> { throw ire("Argument '%s' cannot be frozen; remove frozen<> modifier from '%s'", t, t); });

        if (!rawReturnType.isImplicitlyFrozen() && rawReturnType.isFrozen())
            throw ire("Return type '%s' cannot be frozen; remove frozen<> modifier from '%s'", rawReturnType, rawReturnType);

        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);
        if (null == keyspace)
            throw ire("Keyspace '%s' doesn't exist", keyspaceName);

        List<AbstractType<?>> argumentTypes =
            rawArgumentTypes.stream()
                            .map(t -> t.prepare(keyspaceName, keyspace.types).getType().udfType())
                            .collect(toList());
        AbstractType<?> returnType = rawReturnType.prepare(keyspaceName, keyspace.types).getType().udfType();

        UDFunction function =
            UDFunction.create(new FunctionName(keyspaceName, functionName),
                              argumentNames,
                              argumentTypes,
                              returnType,
                              calledOnNullInput,
                              language,
                              body);

        UserFunction existingFunction = keyspace.userFunctions.find(function.name(), argumentTypes).orElse(null);
        if (null != existingFunction)
        {
            if (existingFunction.isAggregate())
                throw ire("Function '%s' cannot replace an aggregate", functionName);

            if (ifNotExists)
                return schema;

            if (!orReplace)
                throw ire("Function '%s' already exists", functionName);

            if (calledOnNullInput != ((UDFunction) existingFunction).isCalledOnNullInput())
            {
                throw ire("Function '%s' must have %s directive",
                          functionName,
                          calledOnNullInput ? "CALLED ON NULL INPUT" : "RETURNS NULL ON NULL INPUT");
            }

            if (!returnType.isCompatibleWith(existingFunction.returnType()))
            {
                throw ire("Cannot replace function '%s', the new return type %s is not compatible with the return type %s of existing function",
                          functionName,
                          returnType.asCQL3Type(),
                          existingFunction.returnType().asCQL3Type());
            }

            // TODO: update dependent aggregates
        }

        return schema.withAddedOrUpdated(keyspace.withSwapped(keyspace.userFunctions.withAddedOrUpdated(function)));
    }

    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        assert diff.altered.size() == 1;
        FunctionsDiff<UDFunction> udfsDiff = diff.altered.get(0).udfs;

        assert udfsDiff.created.size() + udfsDiff.altered.size() == 1;
        boolean created = !udfsDiff.created.isEmpty();

        return new SchemaChange(created ? Change.CREATED : Change.UPDATED,
                                Target.FUNCTION,
                                keyspaceName,
                                functionName,
                                rawArgumentTypes.stream().map(CQL3Type.Raw::toString).collect(toList()));
    }

    public void authorize(ClientState client)
    {
        FunctionName name = new FunctionName(keyspaceName, functionName);

        if (Schema.instance.findUserFunction(name, Lists.transform(rawArgumentTypes, t -> t.prepare(keyspaceName).getType().udfType())).isPresent() && orReplace)
            client.ensurePermission(Permission.ALTER, FunctionResource.functionFromCql(keyspaceName, functionName, rawArgumentTypes));
        else
            client.ensurePermission(Permission.CREATE, FunctionResource.keyspace(keyspaceName));
    }

    @Override
    Set<IResource> createdResources(KeyspacesDiff diff)
    {
        assert diff.altered.size() == 1;
        FunctionsDiff<UDFunction> udfsDiff = diff.altered.get(0).udfs;

        assert udfsDiff.created.size() + udfsDiff.altered.size() == 1;

        return udfsDiff.created.isEmpty()
             ? ImmutableSet.of()
             : ImmutableSet.of(FunctionResource.functionFromCql(keyspaceName, functionName, rawArgumentTypes));
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.CREATE_FUNCTION, keyspaceName, functionName);
    }

    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, functionName);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final FunctionName name;
        private final List<ColumnIdentifier> argumentNames;
        private final List<CQL3Type.Raw> rawArgumentTypes;
        private final CQL3Type.Raw rawReturnType;
        private final boolean calledOnNullInput;
        private final String language;
        private final String body;
        private final boolean orReplace;
        private final boolean ifNotExists;

        public Raw(FunctionName name,
                   List<ColumnIdentifier> argumentNames,
                   List<CQL3Type.Raw> rawArgumentTypes,
                   CQL3Type.Raw rawReturnType,
                   boolean calledOnNullInput,
                   String language,
                   String body,
                   boolean orReplace,
                   boolean ifNotExists)
        {
            this.name = name;
            this.argumentNames = argumentNames;
            this.rawArgumentTypes = rawArgumentTypes;
            this.rawReturnType = rawReturnType;
            this.calledOnNullInput = calledOnNullInput;
            this.language = language;
            this.body = body;
            this.orReplace = orReplace;
            this.ifNotExists = ifNotExists;
        }

        public CreateFunctionStatement prepare(ClientState state)
        {
            String keyspaceName = name.hasKeyspace() ? name.keyspace : state.getKeyspace();

            return new CreateFunctionStatement(keyspaceName,
                                               name.name,
                                               argumentNames,
                                               rawArgumentTypes,
                                               rawReturnType,
                                               calledOnNullInput,
                                               language,
                                               body,
                                               orReplace,
                                               ifNotExists);
        }
    }
}
