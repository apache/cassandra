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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;
import java.util.List;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Server;

/**
 * A {@code CREATE AGGREGATE} statement parsed from a CQL query.
 */
public final class CreateAggregateStatement extends SchemaAlteringStatement
{
    private final boolean orReplace;
    private final boolean ifNotExists;
    private FunctionName functionName;
    private FunctionName stateFunc;
    private FunctionName finalFunc;
    private final CQL3Type.Raw stateTypeRaw;

    private final List<CQL3Type.Raw> argRawTypes;
    private final Term.Raw ival;

    private List<AbstractType<?>> argTypes;
    private AbstractType<?> returnType;
    private ScalarFunction stateFunction;
    private ScalarFunction finalFunction;
    private ByteBuffer initcond;

    public CreateAggregateStatement(FunctionName functionName,
                                    List<CQL3Type.Raw> argRawTypes,
                                    String stateFunc,
                                    CQL3Type.Raw stateType,
                                    String finalFunc,
                                    Term.Raw ival,
                                    boolean orReplace,
                                    boolean ifNotExists)
    {
        this.functionName = functionName;
        this.argRawTypes = argRawTypes;
        this.stateFunc = new FunctionName(functionName.keyspace, stateFunc);
        this.finalFunc = finalFunc != null ? new FunctionName(functionName.keyspace, finalFunc) : null;
        this.stateTypeRaw = stateType;
        this.ival = ival;
        this.orReplace = orReplace;
        this.ifNotExists = ifNotExists;
    }

    public Prepared prepare()
    {
        argTypes = new ArrayList<>(argRawTypes.size());
        for (CQL3Type.Raw rawType : argRawTypes)
            argTypes.add(prepareType("arguments", rawType));

        AbstractType<?> stateType = prepareType("state type", stateTypeRaw);

        List<AbstractType<?>> stateArgs = stateArguments(stateType, argTypes);

        Function f = Schema.instance.findFunction(stateFunc, stateArgs).orElse(null);
        if (!(f instanceof ScalarFunction))
            throw new InvalidRequestException("State function " + stateFuncSig(stateFunc, stateTypeRaw, argRawTypes) + " does not exist or is not a scalar function");
        stateFunction = (ScalarFunction)f;

        AbstractType<?> stateReturnType = stateFunction.returnType();
        if (!stateReturnType.equals(stateType))
            throw new InvalidRequestException("State function " + stateFuncSig(stateFunction.name(), stateTypeRaw, argRawTypes) + " return type must be the same as the first argument type - check STYPE, argument and return types");

        if (finalFunc != null)
        {
            List<AbstractType<?>> finalArgs = Collections.<AbstractType<?>>singletonList(stateType);
            f = Schema.instance.findFunction(finalFunc, finalArgs).orElse(null);
            if (!(f instanceof ScalarFunction))
                throw new InvalidRequestException("Final function " + finalFunc + '(' + stateTypeRaw + ") does not exist or is not a scalar function");
            finalFunction = (ScalarFunction) f;
            returnType = finalFunction.returnType();
        }
        else
        {
            returnType = stateReturnType;
        }

        if (ival != null)
        {
            initcond = Terms.asBytes(functionName.keyspace, ival.toString(), stateType);

            if (initcond != null)
            {
                try
                {
                    stateType.validate(initcond);
                }
                catch (MarshalException e)
                {
                    throw new InvalidRequestException(String.format("Invalid value for INITCOND of type %s%s", stateType.asCQL3Type(),
                                                                    e.getMessage() == null ? "" : String.format(" (%s)", e.getMessage())));
                }
            }

            // Sanity check that converts the initcond to a CQL literal and parse it back to avoid getting in CASSANDRA-11064.
            String initcondAsCql = stateType.asCQL3Type().toCQLLiteral(initcond, Server.CURRENT_VERSION);
            assert Objects.equals(initcond, Terms.asBytes(functionName.keyspace, initcondAsCql, stateType));

            if (Constants.NULL_LITERAL != ival && UDHelper.isNullOrEmpty(stateType, initcond))
                throw new InvalidRequestException("INITCOND must not be empty for all types except TEXT, ASCII, BLOB");
        }

        return super.prepare();
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

    public void prepareKeyspace(ClientState state) throws InvalidRequestException
    {
        if (!functionName.hasKeyspace() && state.getRawKeyspace() != null)
            functionName = new FunctionName(state.getKeyspace(), functionName.name);

        if (!functionName.hasKeyspace())
            throw new InvalidRequestException("Functions must be fully qualified with a keyspace name if a keyspace is not set for the session");

        ThriftValidation.validateKeyspaceNotSystem(functionName.keyspace);

        stateFunc = new FunctionName(functionName.keyspace, stateFunc.name);
        if (finalFunc != null)
            finalFunc = new FunctionName(functionName.keyspace, finalFunc.name);
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

        for (Function referencedFunction : stateFunction.getFunctions())
            state.ensureHasPermission(Permission.EXECUTE, referencedFunction);

        if (finalFunction != null)
            for (Function referencedFunction : finalFunction.getFunctions())
                state.ensureHasPermission(Permission.EXECUTE, referencedFunction);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        if (ifNotExists && orReplace)
            throw new InvalidRequestException("Cannot use both 'OR REPLACE' and 'IF NOT EXISTS' directives");

        if (Schema.instance.getKSMetaData(functionName.keyspace) == null)
            throw new InvalidRequestException(String.format("Cannot add aggregate '%s' to non existing keyspace '%s'.", functionName.name, functionName.keyspace));
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
            if (!(old instanceof AggregateFunction))
                throw new InvalidRequestException(String.format("Aggregate %s can only replace an aggregate", old));

            // Means we're replacing the function. We still need to validate that 1) it's not a native function and 2) that the return type
            // matches (or that could break existing code badly)
            if (old.isNative())
                throw new InvalidRequestException(String.format("Cannot replace native aggregate %s", old));
            if (!old.returnType().isValueCompatibleWith(returnType))
                throw new InvalidRequestException(String.format("Cannot replace aggregate %s, the new return type %s is not compatible with the return type %s of existing function",
                                                                functionName, returnType.asCQL3Type(), old.returnType().asCQL3Type()));
        }

        if (!stateFunction.isCalledOnNullInput() && initcond == null)
            throw new InvalidRequestException(String.format("Cannot create aggregate %s without INITCOND because state function %s does not accept 'null' arguments", functionName, stateFunc));

        UDAggregate udAggregate = new UDAggregate(functionName, argTypes, returnType, stateFunction, finalFunction, initcond);

        MigrationManager.announceNewAggregate(udAggregate, isLocalOnly);

        return new Event.SchemaChange(replaced ? Event.SchemaChange.Change.UPDATED : Event.SchemaChange.Change.CREATED,
                                      Event.SchemaChange.Target.AGGREGATE,
                                      udAggregate.name().keyspace, udAggregate.name().name, AbstractType.asCQLTypeStringList(udAggregate.argTypes()));
    }

    private static String stateFuncSig(FunctionName stateFuncName, CQL3Type.Raw stateTypeRaw, List<CQL3Type.Raw> argRawTypes)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(stateFuncName.toString()).append('(').append(stateTypeRaw);
        for (CQL3Type.Raw argRawType : argRawTypes)
            sb.append(", ").append(argRawType);
        sb.append(')');
        return sb.toString();
    }

    private static List<AbstractType<?>> stateArguments(AbstractType<?> stateType, List<AbstractType<?>> argTypes)
    {
        List<AbstractType<?>> r = new ArrayList<>(argTypes.size() + 1);
        r.add(stateType);
        r.addAll(argTypes);
        return r;
    }
}
