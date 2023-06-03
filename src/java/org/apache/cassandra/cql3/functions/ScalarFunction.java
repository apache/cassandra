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

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Determines a single output value based on any number of input values.
 */
public interface ScalarFunction extends Function
{
    public boolean isCalledOnNullInput();

    /**
     * Checks if the function is monotonic.
     *
     *<p>A function is monotonic if it is either entirely nonincreasing or nondecreasing given an ordered set of inputs.</p>
     *
     * @return {@code true} if the function is monotonic {@code false} otherwise.
     */
    public default boolean isMonotonic()
    {
        return false;
    }

    /**
     * Applies this function to the specified arguments.
     *
     * @param arguments the input arguments for the function
     * @return the result of applying this function to the arguments
     * @throws InvalidRequestException if this function cannot not be applied to the arguments
     */
    public ByteBuffer execute(Arguments arguments) throws InvalidRequestException;

    /**
     * Does a partial application of the function. That is, given only some of the arguments of the function provided,
     * return a new function that only expect the arguments not provided.
     * <p>
     * To take an example, if you consider the function
     * <pre>
     *     text foo(int a, text b, text c, int d)
     * </pre>
     * then {@code foo.partialApplication([3, <ommitted>, 'bar', <omitted>])} will return a function {@code bar} of signature:
     * <pre>
     *     text bar(text b, int d)
     * </pre>
     * and such that for any value of {@code b} and {@code d}, {@code bar(b, d) == foo(3, b, 'bar', d)}.
     *
     * @param protocolVersion protocol version used for arguments
     * @param partialArguments a list of input arguments for the function where some arguments can be {@link #UNRESOLVED}.
     *                         The input <b>must</b> be of size {@code this.argsType().size()}. For convenience, it is
     *                         allowed both to pass a list with all arguments being {@link #UNRESOLVED} (the function is
     *                         then returned directly) and with none of them unresolved (in which case, if the function is pure,
     *                         it is computed and a dummy no-arg function returning the result is returned).
     * @return a function corresponding to the partial application of this function to the arguments of
     * {@code partialArguments} that are not {@link #UNRESOLVED}.
     */
    default ScalarFunction partialApplication(ProtocolVersion protocolVersion, List<ByteBuffer> partialArguments)
    {
        int unresolvedCount = 0;
        for (ByteBuffer argument : partialArguments)
        {
            if (argument == UNRESOLVED)
                ++unresolvedCount;
        }

        if (unresolvedCount == argTypes().size())
            return this;

        if (isPure() && unresolvedCount == 0)
        {
            Arguments arguments = newArguments(protocolVersion);
            for (int i = 0, m = partialArguments.size(); i < m; i++)
            {
                arguments.set(i, partialArguments.get(i));
            }

            return new PreComputedScalarFunction(returnType(),
                                                 execute(arguments),
                                                 protocolVersion,
                                                 this,
                                                 partialArguments);
        }

        return new PartiallyAppliedScalarFunction(this, partialArguments, unresolvedCount);
    }
}
