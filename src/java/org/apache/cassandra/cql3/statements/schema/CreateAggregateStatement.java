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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.FunctionResource;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.ScalarFunction;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.cql3.functions.UserFunction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.UserFunctions.FunctionsDiff;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;

public final class CreateAggregateStatement extends AlterSchemaStatement
{
    private final String aggregateName;
    private final List<CQL3Type.Raw> rawArgumentTypes;
    private final CQL3Type.Raw rawStateType;
    private final FunctionName stateFunctionName;
    private final FunctionName finalFunctionName;
    private final Term.Raw rawInitialValue;
    private final boolean orReplace;
    private final boolean ifNotExists;

    public CreateAggregateStatement(String keyspaceName,
                                    String aggregateName,
                                    List<CQL3Type.Raw> rawArgumentTypes,
                                    CQL3Type.Raw rawStateType,
                                    FunctionName stateFunctionName,
                                    FunctionName finalFunctionName,
                                    Term.Raw rawInitialValue,
                                    boolean orReplace,
                                    boolean ifNotExists)
    {
        super(keyspaceName);
        this.aggregateName = aggregateName;
        this.rawArgumentTypes = rawArgumentTypes;
        this.rawStateType = rawStateType;
        this.stateFunctionName = stateFunctionName;
        this.finalFunctionName = finalFunctionName;
        this.rawInitialValue = rawInitialValue;
        this.orReplace = orReplace;
        this.ifNotExists = ifNotExists;
    }

    public Keyspaces apply(Keyspaces schema)
    {
        if (ifNotExists && orReplace)
            throw ire("Cannot use both 'OR REPLACE' and 'IF NOT EXISTS' directives");

        if (!FunctionName.isNameValid(aggregateName))
            throw ire("Aggregate name '%s' is invalid", aggregateName);

        rawArgumentTypes.stream()
                        .filter(raw -> !raw.isImplicitlyFrozen() && raw.isFrozen())
                        .findFirst()
                        .ifPresent(t -> { throw ire("Argument '%s' cannot be frozen; remove frozen<> modifier from '%s'", t, t); });

        if (!rawStateType.isImplicitlyFrozen() && rawStateType.isFrozen())
            throw ire("State type '%s' cannot be frozen; remove frozen<> modifier from '%s'", rawStateType, rawStateType);

        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);
        if (null == keyspace)
            throw ire("Keyspace '%s' doesn't exist", keyspaceName);

        /*
         * Resolve the state function
         */

        List<AbstractType<?>> argumentTypes =
            rawArgumentTypes.stream()
                            .map(t -> t.prepare(keyspaceName, keyspace.types).getType().udfType())
                            .collect(toList());
        AbstractType<?> stateType = rawStateType.prepare(keyspaceName, keyspace.types).getType().udfType();
        List<AbstractType<?>> stateFunctionArguments = Lists.newArrayList(concat(singleton(stateType), argumentTypes));

        UserFunction stateFunction =
            keyspace.userFunctions
                    .find(stateFunctionName, stateFunctionArguments)
                    .orElseThrow(() -> ire("State function %s doesn't exist", stateFunctionString()));

        if (stateFunction.isAggregate())
            throw ire("State function %s isn't a scalar function", stateFunctionString());

        if (!stateFunction.returnType().equals(stateType))
        {
            throw ire("State function %s return type must be the same as the first argument type - check STYPE, argument and return types",
                      stateFunctionString());
        }

        /*
         * Resolve the final function and return type
         */

        UserFunction finalFunction = null;
        AbstractType<?> returnType = stateFunction.returnType();

        if (null != finalFunctionName)
        {
            finalFunction = keyspace.userFunctions.find(finalFunctionName, singletonList(stateType)).orElse(null);
            if (null == finalFunction)
                throw ire("Final function %s doesn't exist", finalFunctionString());

            if (finalFunction.isAggregate())
                throw ire("Final function %s isn't a scalar function", finalFunctionString());

            // override return type with that of the final function
            returnType = finalFunction.returnType();
        }

        /*
         * Validate initial condition
         */

        ByteBuffer initialValue = null;
        if (null != rawInitialValue)
        {
            initialValue = Terms.asBytes(keyspaceName, rawInitialValue.toString(), stateType);

            if (null != initialValue)
            {
                try
                {
                    stateType.validate(initialValue);
                }
                catch (MarshalException e)
                {
                    throw ire("Invalid value for INITCOND of type %s", stateType.asCQL3Type());
                }
            }

            // Converts initcond to a CQL literal and parse it back to avoid another CASSANDRA-11064
            String initialValueString = stateType.asCQL3Type().toCQLLiteral(initialValue);
            if (!Objects.equal(initialValue, stateType.asCQL3Type().fromCQLLiteral(keyspaceName, initialValueString)))
                throw new AssertionError(String.format("CQL literal '%s' (from type %s) parsed with a different value", initialValueString, stateType.asCQL3Type()));

            if (Constants.NULL_LITERAL != rawInitialValue && isNullOrEmpty(stateType, initialValue))
                throw ire("INITCOND must not be empty for all types except TEXT, ASCII, BLOB");
        }

        if (!((UDFunction) stateFunction).isCalledOnNullInput() && null == initialValue)
        {
            throw ire("Cannot create aggregate '%s' without INITCOND because state function %s does not accept 'null' arguments",
                      aggregateName,
                      stateFunctionName);
        }

        /*
         * Create or replace
         */

        UDAggregate aggregate =
            new UDAggregate(new FunctionName(keyspaceName, aggregateName),
                            argumentTypes,
                            returnType,
                            (ScalarFunction) stateFunction,
                            (ScalarFunction) finalFunction,
                            initialValue);

        UserFunction existingAggregate = keyspace.userFunctions.find(aggregate.name(), argumentTypes).orElse(null);
        if (null != existingAggregate)
        {
            if (!existingAggregate.isAggregate())
                throw ire("Aggregate '%s' cannot replace a function", aggregateName);

            if (ifNotExists)
                return schema;

            if (!orReplace)
                throw ire("Aggregate '%s' already exists", aggregateName);

            if (!returnType.isCompatibleWith(existingAggregate.returnType()))
            {
                throw ire("Cannot replace aggregate '%s', the new return type %s isn't compatible with the return type %s of existing function",
                          aggregateName,
                          returnType.asCQL3Type(),
                          existingAggregate.returnType().asCQL3Type());
            }
        }

        return schema.withAddedOrUpdated(keyspace.withSwapped(keyspace.userFunctions.withAddedOrUpdated(aggregate)));
    }

    private static boolean isNullOrEmpty(AbstractType<?> type, ByteBuffer bb)
    {
        return bb == null ||
               (bb.remaining() == 0 && type.isEmptyValueMeaningless());
    }

    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        assert diff.altered.size() == 1;
        FunctionsDiff<UDAggregate> udasDiff = diff.altered.get(0).udas;

        assert udasDiff.created.size() + udasDiff.altered.size() == 1;
        boolean created = !udasDiff.created.isEmpty();

        return new SchemaChange(created ? Change.CREATED : Change.UPDATED,
                                Target.AGGREGATE,
                                keyspaceName,
                                aggregateName,
                                rawArgumentTypes.stream().map(CQL3Type.Raw::toString).collect(toList()));
    }

    public void authorize(ClientState client)
    {
        FunctionName name = new FunctionName(keyspaceName, aggregateName);

        if (Schema.instance.findUserFunction(name, Lists.transform(rawArgumentTypes, t -> t.prepare(keyspaceName).getType())).isPresent() && orReplace)
            client.ensurePermission(Permission.ALTER, FunctionResource.functionFromCql(keyspaceName, aggregateName, rawArgumentTypes));
        else
            client.ensurePermission(Permission.CREATE, FunctionResource.keyspace(keyspaceName));

        FunctionResource stateFunction =
            FunctionResource.functionFromCql(stateFunctionName, Lists.newArrayList(concat(singleton(rawStateType), rawArgumentTypes)));
        client.ensurePermission(Permission.EXECUTE, stateFunction);

        if (null != finalFunctionName)
            client.ensurePermission(Permission.EXECUTE, FunctionResource.functionFromCql(finalFunctionName, singletonList(rawStateType)));
    }

    @Override
    Set<IResource> createdResources(KeyspacesDiff diff)
    {
        assert diff.altered.size() == 1;
        FunctionsDiff<UDAggregate> udasDiff = diff.altered.get(0).udas;

        assert udasDiff.created.size() + udasDiff.altered.size() == 1;

        return udasDiff.created.isEmpty()
             ? ImmutableSet.of()
             : ImmutableSet.of(FunctionResource.functionFromCql(keyspaceName, aggregateName, rawArgumentTypes));
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.CREATE_AGGREGATE, keyspaceName, aggregateName);
    }

    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, aggregateName);
    }

    private String stateFunctionString()
    {
        return format("%s(%s)", stateFunctionName, join(", ", transform(concat(singleton(rawStateType), rawArgumentTypes), Object::toString)));
    }

    private String finalFunctionString()
    {
        return format("%s(%s)", finalFunctionName, rawStateType);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final FunctionName aggregateName;
        private final List<CQL3Type.Raw> rawArgumentTypes;
        private final CQL3Type.Raw rawStateType;
        private final String stateFunctionName;
        private final String finalFunctionName;
        private final Term.Raw rawInitialValue;
        private final boolean orReplace;
        private final boolean ifNotExists;

        public Raw(FunctionName aggregateName,
                   List<CQL3Type.Raw> rawArgumentTypes,
                   CQL3Type.Raw rawStateType,
                   String stateFunctionName,
                   String finalFunctionName,
                   Term.Raw rawInitialValue,
                   boolean orReplace,
                   boolean ifNotExists)
        {
            this.aggregateName = aggregateName;
            this.rawArgumentTypes = rawArgumentTypes;
            this.rawStateType = rawStateType;
            this.stateFunctionName = stateFunctionName;
            this.finalFunctionName = finalFunctionName;
            this.rawInitialValue = rawInitialValue;
            this.orReplace = orReplace;
            this.ifNotExists = ifNotExists;
        }

        public CreateAggregateStatement prepare(ClientState state)
        {
            String keyspaceName = aggregateName.hasKeyspace() ? aggregateName.keyspace : state.getKeyspace();

            return new CreateAggregateStatement(keyspaceName,
                                                aggregateName.name,
                                                rawArgumentTypes,
                                                rawStateType,
                                                new FunctionName(keyspaceName, stateFunctionName),
                                                null != finalFunctionName ? new FunctionName(keyspaceName, finalFunctionName) : null,
                                                rawInitialValue,
                                                orReplace,
                                                ifNotExists);
        }
    }
}
