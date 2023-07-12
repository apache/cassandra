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
import org.apache.cassandra.transport.ProtocolVersion;

import static org.apache.cassandra.cql3.functions.TimeFcts.*;

import org.apache.commons.lang3.StringUtils;

/**
 * Casting functions
 *
 */
public final class CastFcts
{
    private static final String FUNCTION_NAME_PREFIX = "cast_as_";

    // Until 5.0 we have used camel cased names for cast functions. That changed with CASSANDRA-18037, where we decided
    // to adopt snake case for all native function names. However, we should still support the old came case names.
    private static final String LEGACY_FUNCTION_NAME_PREFIX = "castAs";

    public static void addFunctionsTo(NativeFunctions functions)
    {
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
            addFunctionIfNeeded(functions, inputType, DecimalType.instance, getDecimalConversionFunction(inputType));
            addFunctionIfNeeded(functions, inputType, IntegerType.instance, p -> BigInteger.valueOf(p.longValue()));
            functions.add(CastAsTextFunction.create(inputType, AsciiType.instance));
            functions.add(CastAsTextFunction.create(inputType, UTF8Type.instance));
        }

        functions.add(JavaFunctionWrapper.create(AsciiType.instance, UTF8Type.instance, p -> p));

        functions.add(CastAsTextFunction.create(InetAddressType.instance, AsciiType.instance));
        functions.add(CastAsTextFunction.create(InetAddressType.instance, UTF8Type.instance));

        functions.add(CastAsTextFunction.create(BooleanType.instance, AsciiType.instance));
        functions.add(CastAsTextFunction.create(BooleanType.instance, UTF8Type.instance));

        functions.add(CassandraFunctionWrapper.create(TimeUUIDType.instance, SimpleDateType.instance, toDate(TimeUUIDType.instance)));
        functions.add(CassandraFunctionWrapper.create(TimeUUIDType.instance, TimestampType.instance, toTimestamp(TimeUUIDType.instance)));
        functions.add(CastAsTextFunction.create(TimeUUIDType.instance, AsciiType.instance));
        functions.add(CastAsTextFunction.create(TimeUUIDType.instance, UTF8Type.instance));
        functions.add(CassandraFunctionWrapper.create(TimestampType.instance, SimpleDateType.instance, toDate(TimestampType.instance)));
        functions.add(CastAsTextFunction.create(TimestampType.instance, AsciiType.instance));
        functions.add(CastAsTextFunction.create(TimestampType.instance, UTF8Type.instance));
        functions.add(CassandraFunctionWrapper.create(SimpleDateType.instance, TimestampType.instance, toTimestamp(SimpleDateType.instance)));
        functions.add(CastAsTextFunction.create(SimpleDateType.instance, AsciiType.instance));
        functions.add(CastAsTextFunction.create(SimpleDateType.instance, UTF8Type.instance));
        functions.add(CastAsTextFunction.create(TimeType.instance, AsciiType.instance));
        functions.add(CastAsTextFunction.create(TimeType.instance, UTF8Type.instance));

        functions.add(CastAsTextFunction.create(UUIDType.instance, AsciiType.instance));
        functions.add(CastAsTextFunction.create(UUIDType.instance, UTF8Type.instance));
    }

    /**
     * Returns the conversion function to convert the specified type into a Decimal type
     *
     * @param inputType the input type
     * @return the conversion function to convert the specified type into a Decimal type
     */
    private static <I extends Number> java.util.function.Function<I, BigDecimal> getDecimalConversionFunction(AbstractType<? extends Number> inputType)
    {
        if (inputType == FloatType.instance)
            return p -> new BigDecimal(Float.toString(p.floatValue()));

        if (inputType == DoubleType.instance)
            return p -> BigDecimal.valueOf(p.doubleValue());

        if (inputType == IntegerType.instance)
            return p -> new BigDecimal((BigInteger) p);

        return p -> BigDecimal.valueOf(p.longValue());
    }

    /**
     * Creates the snake-cased name of the cast function used to cast to the specified type.
     *
     * @param outputType the output type
     * @return the name of the cast function used to cast to the specified type
     */
    public static String getFunctionName(CQL3Type outputType)
    {
        return FUNCTION_NAME_PREFIX + toLowerCaseString(outputType);
    }

    /**
     * Creates the legacy camel-cased name of the cast function used to cast to the specified type.
     *
     * @param outputType the output type
     * @return the legacy camel-cased name of the cast function used to cast to the specified type
     */
    private static String getLegacyFunctionName(CQL3Type outputType)
    {
        return LEGACY_FUNCTION_NAME_PREFIX + StringUtils.capitalize(toLowerCaseString(outputType));
    }

    /**
     * Creates the name of the cast function used to cast to the specified type.
     *
     * @param outputType the output type
     * @param legacy whether to use the old cameCase names, instead of the new snake_case names
     * @return the name of the cast function used to cast to the specified type
     */
    private static String getFunctionName(AbstractType<?> outputType, boolean legacy)
    {
        CQL3Type type = outputType.asCQL3Type();
        return legacy ? getLegacyFunctionName(type) : getFunctionName(type);
    }

    /**
     * Adds to the list a function converting the input type in to the output type if they are not the same.
     *
     * @param functions the list to add to
     * @param inputType the input type
     * @param outputType the output type
     * @param converter the function use to convert the input type into the output type
     */
    private static <I, O> void addFunctionIfNeeded(NativeFunctions functions,
                                                   AbstractType<I> inputType,
                                                   AbstractType<O> outputType,
                                                   java.util.function.Function<I, O> converter)
    {
        if (!inputType.equals(outputType))
            functions.add(wrapJavaFunction(inputType, outputType, converter));
    }

    @SuppressWarnings("unchecked")
    private static <O, I> CastFunction<?, O> wrapJavaFunction(AbstractType<I> inputType,
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
        public CastFunction(AbstractType<I> inputType, AbstractType<O> outputType, boolean useLegacyName)
        {
            super(getFunctionName(outputType, useLegacyName), outputType, inputType);
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
        protected final java.util.function.Function<I, O> converter;

        public static <I, O> JavaFunctionWrapper<I, O> create(AbstractType<I> inputType,
                                                              AbstractType<O> outputType,
                                                              java.util.function.Function<I, O> converter)
        {
            return new JavaFunctionWrapper<>(inputType, outputType, converter, false);
        }

        protected JavaFunctionWrapper(AbstractType<I> inputType,
                                      AbstractType<O> outputType,
                                      java.util.function.Function<I, O> converter,
                                      boolean useLegacyName)
        {
            super(inputType, outputType, useLegacyName);
            this.converter = converter;
        }

        @Override
        public JavaFunctionWrapper<I, O> withLegacyName()
        {
            return new JavaFunctionWrapper<>(inputType(), outputType(), converter, true);
        }

        @Override
        public final ByteBuffer execute(Arguments arguments)
        {
            if (arguments.containsNulls())
                return null;

            return outputType().decompose(converter.apply(arguments.get(0)));
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
            return new JavaCounterFunctionWrapper<>(outputType, converter, false);
        }

        protected JavaCounterFunctionWrapper(AbstractType<O> outputType,
                                             java.util.function.Function<Long, O> converter,
                                             boolean useLegacyName)
        {
            super(CounterColumnType.instance, outputType, converter, useLegacyName);
        }

        @Override
        public JavaCounterFunctionWrapper<O> withLegacyName()
        {
            return new JavaCounterFunctionWrapper<>(outputType(), converter, true);
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
            return new CassandraFunctionWrapper<>(inputType, outputType, delegate, false);
        }

        private CassandraFunctionWrapper(AbstractType<I> inputType,
                                         AbstractType<O> outputType,
                                         NativeScalarFunction delegate,
                                         boolean useLegacyName)
        {
            super(inputType, outputType, useLegacyName);
            assert delegate.argTypes().size() == 1 && inputType.equals(delegate.argTypes().get(0));
            assert outputType.equals(delegate.returnType());
            this.delegate = delegate;
        }

        @Override
        public CassandraFunctionWrapper<I, O> withLegacyName()
        {
            return new CassandraFunctionWrapper<>(inputType(), outputType(), delegate, true);
        }

        @Override
        public ByteBuffer execute(Arguments arguments)
        {
            return delegate.execute(arguments);
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
            return new CastAsTextFunction<>(inputType, outputType, false);
        }

        private CastAsTextFunction(AbstractType<I> inputType, AbstractType<String> outputType, boolean useLegacyName)
        {
            super(inputType, outputType, useLegacyName);
        }

        @Override
        public CastAsTextFunction<I> withLegacyName()
        {
            return new CastAsTextFunction<>(inputType(), outputType(), true);
        }

        @Override
        public Arguments newArguments(ProtocolVersion version)
        {
            return new FunctionArguments(version, (protocolVersion, buffer) -> {
                AbstractType<?> argType = argTypes.get(0);
                if (buffer == null || (!buffer.hasRemaining() && argType.isEmptyValueMeaningless()))
                    return null;

                return argType.getSerializer().toCQLLiteralNoQuote(buffer);
            });
        }

        @Override
        public ByteBuffer execute(Arguments arguments)
        {
            if (arguments.containsNulls())
                return null;

            return outputType().decompose(arguments.get(0));
        }
    }

    /**
     * The class must not be instantiated as it contains only static variables.
     */
    private CastFcts()
    {
    }
}
