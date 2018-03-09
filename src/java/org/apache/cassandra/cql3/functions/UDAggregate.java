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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.TypeCodec;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.Functions;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Base class for user-defined-aggregates.
 */
public class UDAggregate extends AbstractFunction implements AggregateFunction
{
    protected static final Logger logger = LoggerFactory.getLogger(UDAggregate.class);

    private final AbstractType<?> stateType;
    private final TypeCodec stateTypeCodec;
    private final TypeCodec returnTypeCodec;
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
        this.stateType = stateFunc != null ? stateFunc.returnType() : null;
        this.stateTypeCodec = stateType != null ? UDHelper.codecFor(UDHelper.driverType(stateType)) : null;
        this.returnTypeCodec = returnType != null ? UDHelper.codecFor(UDHelper.driverType(returnType)) : null;
        this.initcond = initcond;
    }

    public static UDAggregate create(Functions functions,
                                     FunctionName name,
                                     List<AbstractType<?>> argTypes,
                                     AbstractType<?> returnType,
                                     FunctionName stateFunc,
                                     FunctionName finalFunc,
                                     AbstractType<?> stateType,
                                     ByteBuffer initcond)
    throws InvalidRequestException
    {
        List<AbstractType<?>> stateTypes = new ArrayList<>(argTypes.size() + 1);
        stateTypes.add(stateType);
        stateTypes.addAll(argTypes);
        List<AbstractType<?>> finalTypes = Collections.singletonList(stateType);
        return new UDAggregate(name,
                               argTypes,
                               returnType,
                               resolveScalar(functions, name, stateFunc, stateTypes),
                               finalFunc != null ? resolveScalar(functions, name, finalFunc, finalTypes) : null,
                               initcond);
    }

    public static UDAggregate createBroken(FunctionName name,
                                           List<AbstractType<?>> argTypes,
                                           AbstractType<?> returnType,
                                           ByteBuffer initcond,
                                           InvalidRequestException reason)
    {
        return new UDAggregate(name, argTypes, returnType, null, null, initcond)
        {
            public Aggregate newAggregate() throws InvalidRequestException
            {
                throw new InvalidRequestException(String.format("Aggregate '%s' exists but hasn't been loaded successfully for the following reason: %s. "
                                                                + "Please see the server log for more details",
                                                                this,
                                                                reason.getMessage()));
            }
        };
    }

    public boolean hasReferenceTo(Function function)
    {
        return stateFunction == function || finalFunction == function;
    }

    @Override
    public void addFunctionsTo(List<Function> functions)
    {
        functions.add(this);
        if (stateFunction != null)
        {
            stateFunction.addFunctionsTo(functions);

            if (finalFunction != null)
                finalFunction.addFunctionsTo(functions);
        }
    }

    public boolean isAggregate()
    {
        return true;
    }

    public boolean isNative()
    {
        return false;
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
        return stateType;
    }

    public Aggregate newAggregate() throws InvalidRequestException
    {
        return new Aggregate()
        {
            private long stateFunctionCount;
            private long stateFunctionDuration;

            private Object state;
            private boolean needsInit = true;

            public void addInput(ProtocolVersion protocolVersion, List<ByteBuffer> values) throws InvalidRequestException
            {
                maybeInit(protocolVersion);

                long startTime = System.nanoTime();
                stateFunctionCount++;
                if (stateFunction instanceof UDFunction)
                {
                    UDFunction udf = (UDFunction)stateFunction;
                    if (udf.isCallableWrtNullable(values))
                        state = udf.executeForAggregate(protocolVersion, state, values);
                }
                else
                {
                    throw new UnsupportedOperationException("UDAs only support UDFs");
                }
                stateFunctionDuration += (System.nanoTime() - startTime) / 1000;
            }

            private void maybeInit(ProtocolVersion protocolVersion)
            {
                if (needsInit)
                {
                    state = initcond != null ? UDHelper.deserialize(stateTypeCodec, protocolVersion, initcond.duplicate()) : null;
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
                    return UDFunction.decompose(stateTypeCodec, protocolVersion, state);

                if (finalFunction instanceof UDFunction)
                {
                    UDFunction udf = (UDFunction)finalFunction;
                    Object result = udf.executeForAggregate(protocolVersion, state, Collections.emptyList());
                    return UDFunction.decompose(returnTypeCodec, protocolVersion, result);
                }
                throw new UnsupportedOperationException("UDAs only support UDFs");
            }

            public void reset()
            {
                needsInit = true;
            }
        };
    }

    private static ScalarFunction resolveScalar(Functions functions, FunctionName aName, FunctionName fName, List<AbstractType<?>> argTypes) throws InvalidRequestException
    {
        Optional<Function> fun = functions.find(fName, argTypes);
        if (!fun.isPresent())
            throw new InvalidRequestException(String.format("Referenced state function '%s %s' for aggregate '%s' does not exist",
                                                            fName,
                                                            Arrays.toString(UDHelper.driverTypes(argTypes)),
                                                            aName));

        if (!(fun.get() instanceof ScalarFunction))
            throw new InvalidRequestException(String.format("Referenced state function '%s %s' for aggregate '%s' is not a scalar function",
                                                            fName,
                                                            Arrays.toString(UDHelper.driverTypes(argTypes)),
                                                            aName));
        return (ScalarFunction) fun.get();
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof UDAggregate))
            return false;

        UDAggregate that = (UDAggregate) o;
        return Objects.equal(name, that.name)
            && Functions.typesMatch(argTypes, that.argTypes)
            && Functions.typesMatch(returnType, that.returnType)
            && Objects.equal(stateFunction, that.stateFunction)
            && Objects.equal(finalFunction, that.finalFunction)
            && ((stateType == that.stateType) || ((stateType != null) && stateType.equals(that.stateType, true)))  // ignore freezing
            && Objects.equal(initcond, that.initcond);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, Functions.typeHashCode(argTypes), Functions.typeHashCode(returnType), stateFunction, finalFunction, stateType, initcond);
    }
}
