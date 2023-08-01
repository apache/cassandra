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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Function used internally to hold the pre-computed result of another function.
 * <p>
 * See {@link ScalarFunction#partialApplication(ProtocolVersion, List)} for why this is used.
 * <p>
 * Note : the function is cautious in keeping the protocol version used for the pre-computed value and to
 * fallback to recomputation if the version we get when {@link #execute} is called. I don't think it's truly necessary
 * though as I don't think we actually depend on the protocol version for values anymore (it's remnant of previous
 * transitions). It's not a lot of code to be on safe side though until this is cleaned (assuming we do clean it).
 */
class PreComputedScalarFunction extends NativeScalarFunction implements PartialScalarFunction
{
    private final ByteBuffer value;
    private final ProtocolVersion valueVersion;

    private final ScalarFunction function;
    private final List<ByteBuffer> arguments;

    PreComputedScalarFunction(AbstractType<?> returnType,
                              ByteBuffer value,
                              ProtocolVersion valueVersion,
                              ScalarFunction function,
                              List<ByteBuffer> arguments)
    {
        // Note that we never register those function, there are just used internally, so the name doesn't matter much
        super("__constant__", returnType);
        this.value = value;
        this.valueVersion = valueVersion;
        this.function = function;
        this.arguments = arguments;
    }

    @Override
    public Function getFunction()
    {
        return function;
    }

    @Override
    public List<ByteBuffer> getPartialArguments()
    {
        return arguments;
    }

    @Override
    public ByteBuffer execute(Arguments nothing) throws InvalidRequestException
    {
        if (nothing.getProtocolVersion() == valueVersion)
            return value;

        Arguments args = function.newArguments(nothing.getProtocolVersion());
        for (int i = 0, m = arguments.size() ; i < m; i++)
        {
            args.set(i, arguments.get(i));;
        }

        return function.execute(args);
    }

    public ScalarFunction partialApplication(ProtocolVersion protocolVersion, List<ByteBuffer> nothing) throws InvalidRequestException
    {
        return this;
    }
}
