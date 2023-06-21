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
package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.Difference;
import org.apache.cassandra.schema.UserFunctions;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;

import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Base class for user-defined-aggregates.
 */
public class UDAggregate extends UserFunction implements AggregateFunction
{
    protected static final Logger logger = LoggerFactory.getLogger(UDAggregate.class);

    private final UDFDataType stateType;
    private final List<UDFDataType> argumentTypes;
    private final UDFDataType resultType;
    protected final ByteBuffer initcond;
    private final ScalarFunction stateFunction;
    private final ScalarFunction finalFunction;

    public UDAggregate(FunctionName name,
                       List<AbstractType<?>> argTypes,
                       AbstractType<?> returnType,
                       ScalarFunction stateFunc,
                       ScalarFunction finalFunc,
                       ByteBuffer initcond)
    {
        super(name, argTypes, returnType);
        this.stateFunction = stateFunc;
        this.finalFunction = finalFunc;
        this.argumentTypes = UDFDataType.wrap(argTypes, false);
        this.resultType = UDFDataType.wrap(returnType, false);
        this.stateType = stateFunc != null ? UDFDataType.wrap(stateFunc.returnType(), false) : null;
        this.initcond = initcond;
    }

    public static UDAggregate create(Collection<UDFunction> functions,
                                     FunctionName name,
                                     List<AbstractType<?>> argTypes,
                                     AbstractType<?> returnType,
                                     FunctionName stateFunc,
                                     FunctionName finalFunc,
                                     AbstractType<?> stateType,
                                     ByteBuffer initcond)
    {
        List<AbstractType<?>> stateTypes = new ArrayList<>(argTypes.size() + 1);
        stateTypes.add(stateType);
        stateTypes.addAll(argTypes);
        List<AbstractType<?>> finalTypes = Collections.singletonList(stateType);
        return new UDAggregate(name,
                               argTypes,
                               returnType,
                               findFunction(name, functions, stateFunc, stateTypes),
                               null == finalFunc ? null : findFunction(name, functions, finalFunc, finalTypes),
                               initcond);
    }

    private static UDFunction findFunction(FunctionName udaName, Collection<UDFunction> functions, FunctionName name, List<AbstractType<?>> arguments)
    {
        return functions.stream()
                        .filter(f -> f.name().equals(name) && f.typesMatch(arguments))
                        .findFirst()
                        .orElseThrow(() -> new ConfigurationException(String.format("Unable to find function %s referenced by UDA %s", name, udaName)));
    }

    public boolean isPure()
    {
        // Right now, we have no way to check if an UDA is pure. Due to that we consider them as non pure to avoid any risk.
        return false;
    }

    @Override
    public Arguments newArguments(ProtocolVersion version)
    {
        return FunctionArguments.newInstanceForUdf(version, argumentTypes);
    }

    public boolean hasReferenceTo(Function function)
    {
        return stateFunction == function || finalFunction == function;
    }

    @Override
    public boolean referencesUserType(ByteBuffer name)
    {
        return any(argTypes(), t -> t.referencesUserType(name))
            || returnType.referencesUserType(name)
            || (null != stateType && stateType.toAbstractType().referencesUserType(name))
            || stateFunction.referencesUserType(name)
            || (null != finalFunction && finalFunction.referencesUserType(name));
    }

    public UDAggregate withUpdatedUserType(Collection<UDFunction> udfs, UserType udt)
    {
        if (!referencesUserType(udt.name))
            return this;

        return new UDAggregate(name,
                               Lists.newArrayList(transform(argTypes, t -> t.withUpdatedUserType(udt))),
                               returnType.withUpdatedUserType(udt),
                               findFunction(name, udfs, stateFunction.name(), stateFunction.argTypes()),
                               null == finalFunction ? null : findFunction(name, udfs, finalFunction.name(), finalFunction.argTypes()),
                               initcond);
    }

    @Override
    public void addFunctionsTo(List<Function> functions)
    {
        functions.add(this);

        stateFunction.addFunctionsTo(functions);

        if (finalFunction != null)
            finalFunction.addFunctionsTo(functions);
    }

    public boolean isAggregate()
    {
        return true;
    }

    public ScalarFunction stateFunction()
    {
        return stateFunction;
    }

    public ScalarFunction finalFunction()
    {
        return finalFunction;
    }

    public ByteBuffer initialCondition()
    {
        return initcond;
    }

    public AbstractType<?> stateType()
    {
        return stateType == null ? null : stateType.toAbstractType();
    }

    public Aggregate newAggregate() throws InvalidRequestException
    {
        return new Aggregate()
        {
            private long stateFunctionCount;
            private long stateFunctionDuration;

            private Object state;
            private boolean needsInit = true;

            @Override
            public void addInput(Arguments arguments) throws InvalidRequestException
            {
                maybeInit(arguments.getProtocolVersion());

                long startTime = nanoTime();
                stateFunctionCount++;
                if (stateFunction instanceof UDFunction)
                {
                    UDFunction udf = (UDFunction)stateFunction;
                    if (udf.isCallableWrtNullable(arguments))
                        state = udf.executeForAggregate(state, arguments);
                }
                else
                {
                    throw new UnsupportedOperationException("UDAs only support UDFs");
                }
                stateFunctionDuration += (nanoTime() - startTime) / 1000;
            }

            private void maybeInit(ProtocolVersion protocolVersion)
            {
                if (needsInit)
                {
                    state = initcond != null ? stateType.compose(protocolVersion, initcond.duplicate()) : null;
                    stateFunctionDuration = 0;
                    stateFunctionCount = 0;
                    needsInit = false;
                }
            }

            public ByteBuffer compute(ProtocolVersion protocolVersion) throws InvalidRequestException
            {
                maybeInit(protocolVersion);

                // final function is traced in UDFunction
                Tracing.trace("Executed UDA {}: {} call(s) to state function {} in {}\u03bcs", name(), stateFunctionCount, stateFunction.name(), stateFunctionDuration);
                if (finalFunction == null)
                    return stateType.decompose(protocolVersion, state);

                if (finalFunction instanceof UDFunction)
                {
                    UDFunction udf = (UDFunction)finalFunction;
                    Object result = udf.executeForAggregate(state, FunctionArguments.emptyInstance(protocolVersion));
                    return resultType.decompose(protocolVersion, result);
                }
                throw new UnsupportedOperationException("UDAs only support UDFs");
            }

            public void reset()
            {
                needsInit = true;
            }
        };
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof UDAggregate))
            return false;

        UDAggregate that = (UDAggregate) o;
        return equalsWithoutTypesAndFunctions(that)
            && argTypes.equals(that.argTypes)
            && returnType.equals(that.returnType)
            && Objects.equal(stateFunction, that.stateFunction)
            && Objects.equal(finalFunction, that.finalFunction)
            && ((stateType == that.stateType) || ((stateType != null) && stateType.equals(that.stateType)));
    }

    private boolean equalsWithoutTypesAndFunctions(UDAggregate other)
    {
        return name.equals(other.name)
            && argTypes.size() == other.argTypes.size()
            && Objects.equal(initcond, other.initcond);
    }

    @Override
    public Optional<Difference> compare(Function function)
    {
        if (!(function instanceof UDAggregate))
            throw new IllegalArgumentException();

        UDAggregate other = (UDAggregate) function;

        if (!equalsWithoutTypesAndFunctions(other)
        || ((null == finalFunction) != (null == other.finalFunction))
        || ((null == stateType) != (null == other.stateType)))
            return Optional.of(Difference.SHALLOW);

        boolean differsDeeply = false;

        if (null != finalFunction && !finalFunction.equals(other.finalFunction))
        {
            if (finalFunction.name().equals(other.finalFunction.name()))
                differsDeeply = true;
            else
                return Optional.of(Difference.SHALLOW);
        }

        if (null != stateType && !stateType.equals(other.stateType))
        {
            if (stateType.toAbstractType().asCQL3Type().toString()
                         .equals(other.stateType.toAbstractType().asCQL3Type().toString()))
                differsDeeply = true;
            else
                return Optional.of(Difference.SHALLOW);
        }

        if (!returnType.equals(other.returnType))
        {
            if (returnType.asCQL3Type().toString().equals(other.returnType.asCQL3Type().toString()))
                differsDeeply = true;
            else
                return Optional.of(Difference.SHALLOW);
        }

        for (int i = 0; i < argTypes().size(); i++)
        {
            AbstractType<?> thisType = argTypes.get(i);
            AbstractType<?> thatType = other.argTypes.get(i);

            if (!thisType.equals(thatType))
            {
                if (thisType.asCQL3Type().toString().equals(thatType.asCQL3Type().toString()))
                    differsDeeply = true;
                else
                    return Optional.of(Difference.SHALLOW);
            }
        }

        if (!stateFunction.equals(other.stateFunction))
        {
            if (stateFunction.name().equals(other.stateFunction.name()))
                differsDeeply = true;
            else
                return Optional.of(Difference.SHALLOW);
        }

        return differsDeeply ? Optional.of(Difference.DEEP) : Optional.empty();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, UserFunctions.typeHashCode(argTypes), UserFunctions.typeHashCode(returnType), stateFunction, finalFunction, stateType, initcond);
    }

    @Override
    public SchemaElementType elementType()
    {
        return SchemaElementType.AGGREGATE;
    }

    @Override
    public String toCqlString(boolean withInternals, boolean ifNotExists)
    {
        CqlBuilder builder = new CqlBuilder();
        builder.append("CREATE AGGREGATE ");

        if (ifNotExists)
        {
            builder.append("IF NOT EXISTS ");
        }

        builder.append(name())
               .append('(')
               .appendWithSeparators(argTypes, (b, t) -> b.append(toCqlString(t)), ", ")
               .append(')')
               .newLine()
               .increaseIndent()
               .append("SFUNC ")
               .appendQuotingIfNeeded(stateFunction().name().name)
               .newLine()
               .append("STYPE ")
               .append(toCqlString(stateType()));

        if (finalFunction() != null)
            builder.newLine()
                   .append("FINALFUNC ")
                   .appendQuotingIfNeeded(finalFunction().name().name);

        if (initialCondition() != null)
            builder.newLine()
                   .append("INITCOND ")
                   .append(stateType().asCQL3Type().toCQLLiteral(initialCondition()));

        return builder.append(";")
                      .toString();
    }
}
