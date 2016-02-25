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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.commons.lang3.text.WordUtils;

/**
 * Casting functions
 *
 */
public final class CastFcts
{
    private static final String FUNCTION_NAME_PREFIX = "castAs";

    public static Collection<Function> all()
    {
        List<Function> functions = new ArrayList<>();

        @SuppressWarnings("unchecked")
        final AbstractType<? extends Number>[] numericTypes = new AbstractType[] {ByteType.instance,
                                                                                  ShortType.instance,
                                                                                  Int32Type.instance,
                                                                                  LongType.instance,
                                                                                  FloatType.instance,
                                                                                  DoubleType.instance,
                                                                                  DecimalType.instance,
                                                                                  CounterColumnType.instance,
                                                                                  IntegerType.instance};

        for (AbstractType<? extends Number> inputType : numericTypes)
        {
            addFunctionIfNeeded(functions, inputType, ByteType.instance, Number::byteValue);
            addFunctionIfNeeded(functions, inputType, ShortType.instance, Number::shortValue);
            addFunctionIfNeeded(functions, inputType, Int32Type.instance, Number::intValue);
            addFunctionIfNeeded(functions, inputType, LongType.instance, Number::longValue);
            addFunctionIfNeeded(functions, inputType, FloatType.instance, Number::floatValue);
            addFunctionIfNeeded(functions, inputType, DoubleType.instance, Number::doubleValue);
            addFunctionIfNeeded(functions, inputType, DecimalType.instance, p -> BigDecimal.valueOf(p.doubleValue()));
            addFunctionIfNeeded(functions, inputType, IntegerType.instance, p -> BigInteger.valueOf(p.longValue()));
            functions.add(CastAsTextFunction.create(inputType, AsciiType.instance));
            functions.add(CastAsTextFunction.create(inputType, UTF8Type.instance));
        }

        functions.add(JavaFunctionWrapper.create(AsciiType.instance, UTF8Type.instance, p -> p));

        functions.add(CastAsTextFunction.create(InetAddressType.instance, AsciiType.instance));
        functions.add(CastAsTextFunction.create(InetAddressType.instance, UTF8Type.instance));

        functions.add(CastAsTextFunction.create(BooleanType.instance, AsciiType.instance));
        functions.add(CastAsTextFunction.create(BooleanType.instance, UTF8Type.instance));

        functions.add(CassandraFunctionWrapper.create(TimeUUIDType.instance, SimpleDateType.instance, TimeFcts.timeUuidtoDate));
        functions.add(CassandraFunctionWrapper.create(TimeUUIDType.instance, TimestampType.instance, TimeFcts.timeUuidToTimestamp));
        functions.add(CastAsTextFunction.create(TimeUUIDType.instance, AsciiType.instance));
        functions.add(CastAsTextFunction.create(TimeUUIDType.instance, UTF8Type.instance));
        functions.add(CassandraFunctionWrapper.create(TimestampType.instance, SimpleDateType.instance, TimeFcts.timestampToDate));
        functions.add(CastAsTextFunction.create(TimestampType.instance, AsciiType.instance));
        functions.add(CastAsTextFunction.create(TimestampType.instance, UTF8Type.instance));
        functions.add(CassandraFunctionWrapper.create(SimpleDateType.instance, TimestampType.instance, TimeFcts.dateToTimestamp));
        functions.add(CastAsTextFunction.create(SimpleDateType.instance, AsciiType.instance));
        functions.add(CastAsTextFunction.create(SimpleDateType.instance, UTF8Type.instance));
        functions.add(CastAsTextFunction.create(TimeType.instance, AsciiType.instance));
        functions.add(CastAsTextFunction.create(TimeType.instance, UTF8Type.instance));

        functions.add(CastAsTextFunction.create(UUIDType.instance, AsciiType.instance));
        functions.add(CastAsTextFunction.create(UUIDType.instance, UTF8Type.instance));

        return functions;
    }

    /**
     * Creates the name of the cast function use to cast to the specified type.
     *
     * @param outputType the output type
     * @return the name of the cast function use to cast to the specified type
     */
    public static String getFunctionName(AbstractType<?> outputType)
    {
        return getFunctionName(outputType.asCQL3Type());
    }

    /**
     * Creates the name of the cast function use to cast to the specified type.
     *
     * @param outputType the output type
     * @return the name of the cast function use to cast to the specified type
     */
    public static String getFunctionName(CQL3Type outputType)
    {
        return FUNCTION_NAME_PREFIX + WordUtils.capitalize(toLowerCaseString(outputType));
    }

    /**
     * Adds to the list a function converting the input type in to the output type if they are not the same.
     *
     * @param functions the list to add to
     * @param inputType the input type
     * @param outputType the output type
     * @param converter the function use to convert the input type into the output type
     */
    private static <I, O> void addFunctionIfNeeded(List<Function> functions,
                                                   AbstractType<I> inputType,
                                                   AbstractType<O> outputType,
                                                   java.util.function.Function<I, O> converter)
    {
        if (!inputType.equals(outputType))
            functions.add(wrapJavaFunction(inputType, outputType, converter));
    }

    @SuppressWarnings("unchecked")
    private static <O, I> Function wrapJavaFunction(AbstractType<I> inputType,
                                                    AbstractType<O> outputType,
                                                    java.util.function.Function<I, O> converter)
    {
        return inputType.equals(CounterColumnType.instance)
                ? JavaCounterFunctionWrapper.create(outputType, (java.util.function.Function<Long, O>) converter)
                : JavaFunctionWrapper.create(inputType, outputType, converter);
    }

    private static String toLowerCaseString(CQL3Type type)
    {
        return type.toString().toLowerCase();
    }

    /**
     * Base class for the CAST functions.
     *
     * @param <I> the input type
     * @param <O> the output type
     */
    private static abstract class CastFunction<I, O> extends NativeScalarFunction
    {
        public CastFunction(AbstractType<I> inputType, AbstractType<O> outputType)
        {
            super(getFunctionName(outputType), outputType, inputType);
        }

        @Override
        public String columnName(List<String> columnNames)
        {
            return String.format("cast(%s as %s)", columnNames.get(0), toLowerCaseString(outputType().asCQL3Type()));
        }

        @SuppressWarnings("unchecked")
        protected AbstractType<O> outputType()
        {
            return (AbstractType<O>) returnType;
        }

        @SuppressWarnings("unchecked")
        protected AbstractType<I> inputType()
        {
            return (AbstractType<I>) argTypes.get(0);
        }
    }

    /**
     * <code>CastFunction</code> that implements casting by wrapping a java <code>Function</code>.
     *
     * @param <I> the input parameter
     * @param <O> the output parameter
     */
    private static class JavaFunctionWrapper<I, O> extends CastFunction<I, O>
    {
        /**
         * The java function used to convert the input type into the output one.
         */
        private final java.util.function.Function<I, O> converter;

        public static <I, O> JavaFunctionWrapper<I, O> create(AbstractType<I> inputType,
                                                              AbstractType<O> outputType,
                                                              java.util.function.Function<I, O> converter)
        {
            return new JavaFunctionWrapper<I, O>(inputType, outputType, converter);
        }

        protected JavaFunctionWrapper(AbstractType<I> inputType,
                                      AbstractType<O> outputType,
                                      java.util.function.Function<I, O> converter)
        {
            super(inputType, outputType);
            this.converter = converter;
        }

        public final ByteBuffer execute(int protocolVersion, List<ByteBuffer> parameters)
        {
            ByteBuffer bb = parameters.get(0);
            if (bb == null)
                return null;

            return outputType().decompose(converter.apply(compose(bb)));
        }

        protected I compose(ByteBuffer bb)
        {
            return inputType().compose(bb);
        }
    }

    /**
     * <code>JavaFunctionWrapper</code> for counter columns.
     *
     * <p>Counter columns need to be handled in a special way because their binary representation is converted into
     * the one of a BIGINT before functions are applied.</p>
     *
     * @param <O> the output parameter
     */
    private static class JavaCounterFunctionWrapper<O> extends JavaFunctionWrapper<Long, O>
    {
        public static <O> JavaFunctionWrapper<Long, O> create(AbstractType<O> outputType,
                                                              java.util.function.Function<Long, O> converter)
        {
            return new JavaCounterFunctionWrapper<O>(outputType, converter);
        }

        protected JavaCounterFunctionWrapper(AbstractType<O> outputType,
                                            java.util.function.Function<Long, O> converter)
        {
            super(CounterColumnType.instance, outputType, converter);
        }

        protected Long compose(ByteBuffer bb)
        {
            return LongType.instance.compose(bb);
        }
    }

    /**
     * <code>CastFunction</code> that implements casting by wrapping an existing <code>NativeScalarFunction</code>.
     *
     * @param <I> the input parameter
     * @param <O> the output parameter
     */
    private static final class CassandraFunctionWrapper<I, O> extends CastFunction<I, O>
    {
        /**
         * The native scalar function used to perform the conversion.
         */
        private final NativeScalarFunction delegate;

        public static <I, O> CassandraFunctionWrapper<I, O> create(AbstractType<I> inputType,
                                                                   AbstractType<O> outputType,
                                                                   NativeScalarFunction delegate)
        {
            return new CassandraFunctionWrapper<I, O>(inputType, outputType, delegate);
        }

        private CassandraFunctionWrapper(AbstractType<I> inputType,
                                         AbstractType<O> outputType,
                                         NativeScalarFunction delegate)
        {
            super(inputType, outputType);
            assert delegate.argTypes().size() == 1 && inputType.equals(delegate.argTypes().get(0));
            assert outputType.equals(delegate.returnType());
            this.delegate = delegate;
        }

        public ByteBuffer execute(int protocolVersion, List<ByteBuffer> parameters)
        {
            return delegate.execute(protocolVersion, parameters);
        }
    }

    /**
     * <code>CastFunction</code> that can be used to cast a type into ascii or text types.
     *
     * @param <I> the input parameter
     */
    private static final class CastAsTextFunction<I> extends CastFunction<I, String>
    {

        public static <I> CastAsTextFunction<I> create(AbstractType<I> inputType,
                                                       AbstractType<String> outputType)
        {
            return new CastAsTextFunction<I>(inputType, outputType);
        }

        private CastAsTextFunction(AbstractType<I> inputType,
                                    AbstractType<String> outputType)
        {
            super(inputType, outputType);
        }

        public ByteBuffer execute(int protocolVersion, List<ByteBuffer> parameters)
        {
            ByteBuffer bb = parameters.get(0);
            if (bb == null)
                return null;

            return outputType().decompose(inputType().getSerializer().toCQLLiteral(bb));
        }
    }

    /**
     * The class must not be instantiated as it contains only static variables.
     */
    private CastFcts()
    {
    }
}
