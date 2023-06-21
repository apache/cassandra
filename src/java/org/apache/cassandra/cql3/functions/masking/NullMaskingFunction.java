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

package org.apache.cassandra.cql3.functions.masking;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.cql3.functions.Arguments;
import org.apache.cassandra.cql3.functions.FunctionArguments;
import org.apache.cassandra.cql3.functions.FunctionFactory;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.FunctionParameter;
import org.apache.cassandra.cql3.functions.NativeFunction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * A {@link MaskingFunction} that always returns a {@code null} column. The returned value is always an absent column,
 * as it didn't exist, and not a not-null column representing a {@code null} value.
 * <p>
 * For example, given a text column named "username", {@code mask_null(username)} will always return {@code null},
 * independently of the actual value of that column.
 */
public class NullMaskingFunction extends MaskingFunction
{
    public static final String NAME = "null";

    private NullMaskingFunction(FunctionName name, AbstractType<?> inputType)
    {
        super(name, inputType, inputType);
    }

    @Override
    public Arguments newArguments(ProtocolVersion version)
    {
        return FunctionArguments.newNoopInstance(version, 1);
    }

    @Override
    public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
    {
        return null;
    }

    /** @return a {@link FunctionFactory} to build new {@link NullMaskingFunction}s. */
    public static FunctionFactory factory()
    {
        return new MaskingFunction.Factory(NAME, FunctionParameter.anyType(false))
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                return new NullMaskingFunction(name, argTypes.get(0));
            }
        };
    }
}
