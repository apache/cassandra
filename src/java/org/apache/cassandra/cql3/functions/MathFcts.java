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
import java.util.function.BiFunction;

import com.google.common.collect.ImmutableList;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;

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
                     .forEach(functions::add);
    }

    public static NativeFunction absFct(final NumberType<?> type)
    {
        return mathFct("abs", type, NumberType::abs);
    }

    public static NativeFunction expFct(final NumberType<?> type)
    {
        return mathFct("exp", type, NumberType::exp);
    }

    public static NativeFunction logFct(final NumberType<?> type)
    {
        return mathFct("log", type, NumberType::log);
    }

    public static NativeFunction log10Fct(final NumberType<?> type)
    {
        return mathFct("log10", type, NumberType::log10);
    }

    public static NativeFunction roundFct(final NumberType<?> type)
    {
        return mathFct("round", type, NumberType::round);
    }

    private static NativeFunction mathFct(String name,
                                          NumberType<?> type,
                                          BiFunction<NumberType<?>, Number, ByteBuffer> f)
    {
        return new NativeScalarFunction(name, type, type)
        {
            @Override
            public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
            {
                Number number = arguments.get(0);

                if (number == null)
                    return null;

                return f.apply(type, number);
            }
        };
    }

    private MathFcts()
    {
    }
}
