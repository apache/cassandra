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
import java.util.List;

import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * An internal function used to hold the partial application of another function to only some of its parameters.
 *
 * @see ScalarFunction#partialApplication(ProtocolVersion, List)
 */
final class PartiallyAppliedScalarFunction extends NativeScalarFunction implements PartialScalarFunction
{
    private final ScalarFunction function;
    private final List<ByteBuffer> partialParameters;

    PartiallyAppliedScalarFunction(ScalarFunction function, List<ByteBuffer> partialParameters, int unresolvedCount)
    {
        // Note that we never register those function, there are just used internally, so the name doesn't matter much
        super("__partial_application__", function.returnType(), computeArgTypes(function, partialParameters, unresolvedCount));
        this.function = function;
        this.partialParameters = partialParameters;
    }

    @Override
    public boolean isMonotonic()
    {
        return function.isNative() ? ((NativeScalarFunction) function).isPartialApplicationMonotonic(partialParameters)
                                   : function.isMonotonic();
    }

    @Override
    public boolean isPure()
    {
        return function.isPure();
    }

    @Override
    public Function getFunction()
    {
        return function;
    }

    @Override
    public Arguments newArguments(ProtocolVersion version)
    {
        return new PartialFunctionArguments(version, function, partialParameters, argTypes.size());
    }

    @Override
    public List<ByteBuffer> getPartialArguments()
    {
        return partialParameters;
    }

    private static AbstractType<?>[] computeArgTypes(ScalarFunction function, List<ByteBuffer> partialParameters, int unresolvedCount)
    {
        AbstractType<?>[] argTypes = new AbstractType<?>[unresolvedCount];
        int arg = 0;
        for (int i = 0; i < partialParameters.size(); i++)
        {
            if (partialParameters.get(i) == UNRESOLVED)
                argTypes[arg++] = function.argTypes().get(i);
        }
        return argTypes;
    }

    @Override
    public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
    {
        return function.execute(arguments);
    }

    @Override
    public String toString()
    {
        CqlBuilder b = new CqlBuilder().append(function.name()).append(" : (");

        List<AbstractType<?>> types = function.argTypes();
        for (int i = 0, m = types.size(); i < m; i++)
        {
            if (i > 0)
                b.append(", ");
            b.append(toCqlString(types.get(i)));
            if (partialParameters.get(i) != Function.UNRESOLVED)
                b.append("(constant)");
        }
        b.append(") -> ").append(returnType);
        return b.toString();
    }

    /**
     * The arguments for a {@code PartiallyAppliedScalarFunction}.
     *
     */
    private static final class PartialFunctionArguments implements Arguments
    {
        /**
         * The decorated arguments.
         */
        private final Arguments arguments;

        /**
         * The position of the unresolved arguments.
         */
        private final int[] mapping;

        public PartialFunctionArguments(ProtocolVersion version, ScalarFunction function, List<ByteBuffer> partialArguments, int unresolvedCount)
        {
            arguments = function.newArguments(version);
            mapping = new int[unresolvedCount];
            int mappingIndex = 0;
            for (int i = 0, m = partialArguments.size(); i < m; i++)
            {
                ByteBuffer argument = partialArguments.get(i);
                if (argument != Function.UNRESOLVED)
                {
                    arguments.set(i, argument);
                }
                else
                {
                    mapping[mappingIndex++] = i;
                }
            }
        }

        @Override
        public ProtocolVersion getProtocolVersion()
        {
            return arguments.getProtocolVersion();
        }

        @Override
        public void set(int i, ByteBuffer buffer)
        {
            arguments.set(mapping[i], buffer);
        }

        @Override
        public boolean containsNulls()
        {
            return arguments.containsNulls();
        }

        @Override
        public <T> T get(int i)
        {
            return arguments.get(i);
        }

        @Override
        public int size()
        {
            return arguments.size();
        }
    }
}
