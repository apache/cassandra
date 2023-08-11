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
 * A {@link MaskingFunction} that returns a fixed replacement value for the data type of its single argument.
 * <p>
 * The default values are defined by {@link AbstractType#getMaskedValue()}, being {@code ****} for text fields,
 * {@code false} for booleans, zero for numeric types, {@code 1970-01-01} for dates, etc.
 * <p>
 * For example, given a text column named "username", {@code mask_default(username)} will always return {@code ****},
 * independently of the actual value of that column.
 */
public class DefaultMaskingFunction extends MaskingFunction
{
    public static final String NAME = "default";

    AbstractType<?> inputType;

    private DefaultMaskingFunction(FunctionName name, AbstractType<?> inputType)
    {
        super(name, inputType, inputType);
        this.inputType = inputType;
    }

    @Override
    public Arguments newArguments(ProtocolVersion version)
    {
        return FunctionArguments.newNoopInstance(version, 1);
    }

    @Override
    public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
    {
        return inputType.getMaskedValue();
    }

    /** @return a {@link FunctionFactory} to build new {@link DefaultMaskingFunction}s. */
    public static FunctionFactory factory()
    {
        return new MaskingFunction.Factory(NAME, FunctionParameter.anyType(false))
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                return new DefaultMaskingFunction(name, argTypes.get(0));
            }
        };
    }
}
