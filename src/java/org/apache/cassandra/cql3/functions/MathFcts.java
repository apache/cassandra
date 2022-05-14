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
import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.transport.ProtocolVersion;

public final class MathFcts
{
    public static void addFunctionsTo(NativeFunctions functions)
    {
        final List<NumberType<?>> numericTypes = ImmutableList.of(ByteType.instance,
                                                                  ShortType.instance,
                                                                  Int32Type.instance,
                                                                  FloatType.instance,
                                                                  LongType.instance,
                                                                  DoubleType.instance,
                                                                  IntegerType.instance,
                                                                  DecimalType.instance,
                                                                  CounterColumnType.instance);

        numericTypes.stream()
                    .map(t -> ImmutableList.of(absFct(t),
                                               expFct(t),
                                               logFct(t),
                                               log10Fct(t),
                                               roundFct(t)))
                     .flatMap(Collection::stream)
                     .forEach(f -> functions.add(f));
    }

    public static NativeFunction absFct(final NumberType<?> type)
    {
        return new NativeScalarFunction("abs", type, type)
        {
            @Override
            public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
            {
                ByteBuffer bb = parameters.get(0);
                if (bb == null)
                    return null;
                return type.abs(bb);
            }
        };
    }

    public static NativeFunction expFct(final NumberType<?> type)
    {
        return new NativeScalarFunction("exp", type, type)
        {
            @Override
            public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
            {
                ByteBuffer bb = parameters.get(0);
                if (bb == null)
                    return null;
                return type.exp(bb);
            }
        };
    }

    public static NativeFunction logFct(final NumberType<?> type)
    {
        return new NativeScalarFunction("log", type, type)
        {
            @Override
            public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
            {
                ByteBuffer bb = parameters.get(0);
                if (bb == null)
                    return null;
                return type.log(bb);

            }
        };
    }

    public static NativeFunction log10Fct(final NumberType<?> type)
    {
        return new NativeScalarFunction("log10", type, type)
        {
            @Override
            public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
            {
                ByteBuffer bb = parameters.get(0);
                if (bb == null)
                    return null;
                return type.log10(bb);

            }
        };
    }

    public static NativeFunction roundFct(final NumberType<?> type)
    {
        return new NativeScalarFunction("round", type, type)
        {
            @Override
            public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
            {
                ByteBuffer bb = parameters.get(0);
                if (bb == null)
                    return null;
                return type.round(bb);

            }
        };
    }

    private MathFcts()
    {
    }
}
