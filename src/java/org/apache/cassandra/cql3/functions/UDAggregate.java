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
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * Base class for user-defined-aggregates.
 */
public class UDAggregate extends AbstractFunction implements AggregateFunction
{
    protected static final Logger logger = LoggerFactory.getLogger(UDAggregate.class);

    protected final AbstractType<?> stateType;
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
        this.initcond = initcond;
    }

    public static UDAggregate create(FunctionName name,
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
        List<AbstractType<?>> finalTypes = Collections.<AbstractType<?>>singletonList(stateType);
        return new UDAggregate(name,
                               argTypes,
                               returnType,
                               resolveScalar(name, stateFunc, stateTypes),
                               finalFunc != null ? resolveScalar(name, finalFunc, finalTypes) : null,
                               initcond);
    }

    public static UDAggregate createBroken(FunctionName name,
                                           List<AbstractType<?>> argTypes,
                                           AbstractType<?> returnType,
                                           ByteBuffer initcond,
                                           final InvalidRequestException reason)
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

    public Iterable<Function> getFunctions()
    {
        if (stateFunction == null)
            return Collections.emptySet();
        if (finalFunction != null)
            return ImmutableSet.of(this, stateFunction, finalFunction);
        else
            return ImmutableSet.of(this, stateFunction);
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
            private ByteBuffer state;
            {
                reset();
            }

            public void addInput(int protocolVersion, List<ByteBuffer> values) throws InvalidRequestException
            {
                List<ByteBuffer> fArgs = new ArrayList<>(values.size() + 1);
                fArgs.add(state);
                fArgs.addAll(values);
                if (stateFunction instanceof UDFunction)
                {
                    UDFunction udf = (UDFunction)stateFunction;
                    if (udf.isCallableWrtNullable(fArgs))
                        state = udf.executeUserDefined(protocolVersion, fArgs);
                }
                else
                {
                    state = stateFunction.execute(protocolVersion, fArgs);
                }
            }

            public ByteBuffer compute(int protocolVersion) throws InvalidRequestException
            {
                if (finalFunction == null)
                    return state;
                List<ByteBuffer> fArgs = Collections.singletonList(state);
                return finalFunction.execute(protocolVersion, fArgs);
            }

            public void reset()
            {
                state = initcond != null ? initcond.duplicate() : null;
            }
        };
    }

    private static ScalarFunction resolveScalar(FunctionName aName, FunctionName fName, List<AbstractType<?>> argTypes) throws InvalidRequestException
    {
        Function func = Functions.find(fName, argTypes);
        if (func == null)
            throw new InvalidRequestException(String.format("Referenced state function '%s %s' for aggregate '%s' does not exist",
                                                            fName, Arrays.toString(UDHelper.driverTypes(argTypes)), aName));
        if (!(func instanceof ScalarFunction))
            throw new InvalidRequestException(String.format("Referenced state function '%s %s' for aggregate '%s' is not a scalar function",
                                                            fName, Arrays.toString(UDHelper.driverTypes(argTypes)), aName));
        return (ScalarFunction) func;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof UDAggregate))
            return false;

        UDAggregate that = (UDAggregate) o;
        return Objects.equal(name, that.name)
            && Functions.typeEquals(argTypes, that.argTypes)
            && Functions.typeEquals(returnType, that.returnType)
            && Objects.equal(stateFunction, that.stateFunction)
            && Objects.equal(finalFunction, that.finalFunction)
            && Objects.equal(stateType, that.stateType)
            && Objects.equal(initcond, that.initcond);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, argTypes, returnType, stateFunction, finalFunction, stateType, initcond);
    }
}
