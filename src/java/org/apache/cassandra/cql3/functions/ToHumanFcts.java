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

import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import org.apache.commons.lang3.time.DurationUtils;

import org.apache.cassandra.config.DataStorageSpec.DataStorageUnit;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.BYTES;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.GIBIBYTES;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.KIBIBYTES;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.MEBIBYTES;
import static org.apache.cassandra.cql3.CQL3Type.Native.ASCII;
import static org.apache.cassandra.cql3.CQL3Type.Native.BIGINT;
import static org.apache.cassandra.cql3.CQL3Type.Native.INT;
import static org.apache.cassandra.cql3.CQL3Type.Native.SMALLINT;
import static org.apache.cassandra.cql3.CQL3Type.Native.TEXT;
import static org.apache.cassandra.cql3.CQL3Type.Native.TINYINT;
import static org.apache.cassandra.cql3.CQL3Type.Native.VARINT;
import static org.apache.cassandra.cql3.functions.FunctionParameter.fixed;
import static org.apache.cassandra.cql3.functions.FunctionParameter.optional;

public class ToHumanFcts
{
    public static void addFunctionsTo(NativeFunctions functions)
    {
        functions.add(ToHumanSizeFct.factory());
        functions.add(ToHumanDurationFct.factory());
    }

    /**
     * Converts numeric value in a column to a duration value of specified unit.
     * <p>
     * If the function call contains just one argument - value to convert - then it will be
     * looked at as the value is of unit 'ms' and it will be converted to a value of a unit which is closes to it. E.g.
     * If a value is (60 * 1000 + 1) then the unit will be in minutes and converted value will be 1.
     * <p>
     * If the function call contains two arguments - value to convert and a unit - then it will be looked at
     * as the unit of such value is 'ms' and it will be converted into the value of the second (unit) argument.
     * <p>
     * If the function call contains three arguments - value to covert and source and target unit - then the value
     * will be considered of a unit of the second argument, and it will be converted
     * into a value of the third (unit) argument.
     * <p>
     * Examples:
     * <pre>
     * to_human_duration(val)
     * to_human_duration(val, 'm') = to_human_duration(val, 'ms', 'm')
     * to_human_duration(val, 's', 'm')
     * to_human_duration(val, 's', 'h')
     * to_human_duration(val, 's', 'd')
     * to_human_duration(val, 's') = to_human_size(val, 'ms', 's')
     * to_human_duration(val, 'h') = to_human_size(val, 'ms', 'h')
     * </pre>
     * <p>
     * It is possible to convert values of a bigger unit to values of a smaller unit, e.g. this is possible:
     *
     * <pre>
     * to_human_duration(val, 'm', 's')
     * </pre>
     * <p>
     * Values can be max of Long.MAX_VALUE, If the conversion produces overflown value, Long.MAX_VALUE will be returned.
     * <p>
     * Supported units are: d, h, m, s, ms, us, µs, ns
     * <p>
     * Supported column types on which this function is possible to be applied:
     * <pre>INT, TINYINT, SMALLINT, BIGINT, VARINT, ASCII, TEXT</pre>
     * For ASCII and TEXT types, text of such column has to be a non-negative number.
     * <p>
     * The conversion of negative values is not supported.
     */
    public static class ToHumanDurationFct extends NativeScalarFunction
    {
        private static final String FUNCTION_NAME = "to_human_duration";

        private ToHumanDurationFct(AbstractType<?>... argsTypes)
        {
            super(FUNCTION_NAME, UTF8Type.instance, argsTypes);
        }

        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            if (arguments.get(0) == null)
                return null;

            if (arguments.containsNulls())
                throw new InvalidRequestException("none of the arguments may be null");

            long value = getValue(arguments);

            if (value < 0)
                throw new InvalidRequestException("value must be non-negative");

            if (arguments.size() == 1)
            {
                Pair<Long, String> convertedValue = convertValue(value);
                return UTF8Type.instance.fromString(convertedValue.left + " " + convertedValue.right);
            }

            TimeUnit sourceUnit;
            TimeUnit targetUnit;
            String targetUnitAsString;

            if (arguments.size() == 2)
            {
                sourceUnit = MILLISECONDS;
                targetUnitAsString = arguments.get(1);
            }
            else
            {
                sourceUnit = validateUnit(arguments.get(1));
                targetUnitAsString = arguments.get(2);
            }

            targetUnit = validateUnit(targetUnitAsString);

            long convertedValue = convertValue(value, sourceUnit, targetUnit);
            return UTF8Type.instance.fromString(convertedValue + " " + targetUnitAsString);
        }

        private TimeUnit validateUnit(String unitAsString)
        {
            try
            {
                return DurationSpec.fromSymbol(unitAsString);
            }
            catch (Exception ex)
            {
                throw new InvalidRequestException(ex.getMessage());
            }
        }

        private Pair<Long, String> convertValue(long valueToConvert)
        {
            long value;
            if ((value = convertValue(valueToConvert, MILLISECONDS, DAYS)) > 0)
                return Pair.create(value, "d");
            else if ((value = convertValue(valueToConvert, MILLISECONDS, HOURS)) > 0)
                return Pair.create(value, "h");
            else if ((value = convertValue(valueToConvert, MILLISECONDS, MINUTES)) > 0)
                return Pair.create(value, "m");
            else if ((value = convertValue(valueToConvert, MILLISECONDS, SECONDS)) > 0)
                return Pair.create(value, "s");
            else
                return Pair.create(valueToConvert, "ms");
        }

        private long convertValue(long valueToConvert, TimeUnit sourceUnit, TimeUnit targetUnit)
        {
            Duration duration = DurationUtils.toDuration(valueToConvert, sourceUnit);

            if (targetUnit == NANOSECONDS)
                return x(duration::toNanos);
            if (targetUnit == MICROSECONDS)
                return x(() -> duration.toNanos() / 1000);
            else if (targetUnit == MILLISECONDS)
                return x(duration::toMillis);
            else if (targetUnit == SECONDS)
                return x(duration::toSeconds);
            else if (targetUnit == MINUTES)
                return x(duration::toMinutes);
            else if (targetUnit == HOURS)
                return x(duration::toHours);
            else if (targetUnit == DAYS)
                return x(duration::toDays);
            else
                throw new InvalidRequestException("unsupported target unit " + targetUnit);
        }

        // This has a short name to make above code more readable, same pattern as for DataStorageSpec
        private long x(LongSupplier longSupplier)
        {
            try
            {
                return longSupplier.getAsLong();
            }
            catch (ArithmeticException ex)
            {
                return Long.MAX_VALUE;
            }
        }

        public static FunctionFactory factory()
        {
            return new FunctionFactory(FUNCTION_NAME,
                                       fixed(INT, TINYINT, SMALLINT, BIGINT, VARINT, ASCII, TEXT),
                                       optional(fixed(ASCII)),
                                       optional(fixed(ASCII)))
            {
                @Override
                protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
                {
                    if (argTypes.isEmpty() || argTypes.size() > 3)
                        throw invalidNumberOfArgumentsException();

                    return new ToHumanDurationFct(argTypes.toArray(new AbstractType<?>[0]));
                }
            };
        }
    }

    private static final DecimalFormat decimalFormat;

    static
    {
        decimalFormat = new DecimalFormat("0");
        decimalFormat.setRoundingMode(RoundingMode.DOWN);
    }

    /**
     * Converts numeric value in a column to a size value of specified unit.
     * <p>
     * If the function call contains just one argument - value to convert - then it will be
     * looked at as the value is of unit 'B' and it will be converted to a value of a unit which is closest to it. E.g.
     * If a value is (1024 * 1024 + 1) then the unit will be in MiB and converted value will be 1.
     * <p>
     * If the function call contains two arguments - value to convert and a unit - then it will be looked at
     * as the unit of such value is 'B' and it will be converted into the value of the second (unit) argument.
     * <p>
     * If the function call contains three arguments - value to covert and source and target unit - then the value
     * will be considered of a unit of the second argument, and it will be converted
     * into a value of the third (unit) argument.
     * <p>
     * Examples:
     * <pre>
     * to_human_size(val) = to_human_size(val, 'B', 'MiB')
     * to_human_size(val, 'B', 'MiB')
     * to_human_size(val, 'B', 'GiB')
     * to_human_size(val, 'KiB', 'GiB')
     * to_human_size(val, 'MiB') = to_human_size(val, 'B', 'MiB')
     * to_human_size(val, 'GiB') = to_human_size(val, 'B', 'GiB')
     * </pre>
     * <p>
     * It is possible to convert values of a bigger unit to values of a smaller unit, e.g. this is possible:
     *
     * <pre>
     * to_human_size(val, 'GiB', 'B')
     * </pre>
     * <p>
     * Values can be max of Long.MAX_VALUE, If the conversion produces overflown value, Long.MAX_VALUE will be returned.
     * <p>
     * Supported units are: B, KiB, MiB, GiB
     * <p>
     * Supported column types on which this function is possible to be applied:
     * <pre>INT, TINYINT, SMALLINT, BIGINT, VARINT, ASCII, TEXT</pre>
     * For ASCII and TEXT types, text of such column has to be a non-negative number.
     * <p>
     *
     * The conversion of negative values is not supported.
     */
    public static class ToHumanSizeFct extends NativeScalarFunction
    {
        private static final String FUNCTION_NAME = "to_human_size";

        private ToHumanSizeFct(AbstractType<?>... argsTypes)
        {
            super(FUNCTION_NAME, UTF8Type.instance, argsTypes);
        }

        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            if (arguments.get(0) == null)
                return null;

            if (arguments.containsNulls())
                throw new InvalidRequestException("none of the arguments may be null");

            long value = getValue(arguments);

            if (value < 0)
                throw new InvalidRequestException("value must be non-negative");

            DataStorageUnit sourceUnit;
            DataStorageUnit targetUnit;

            if (arguments.size() == 1)
            {
                sourceUnit = BYTES;

                if (value > FileUtils.ONE_GIB)
                    targetUnit = GIBIBYTES;
                else if (value > FileUtils.ONE_MIB)
                    targetUnit = MEBIBYTES;
                else if (value > FileUtils.ONE_KIB)
                    targetUnit = KIBIBYTES;
                else
                    targetUnit = BYTES;
            }
            else if (arguments.size() == 2)
            {
                sourceUnit = BYTES;
                targetUnit = validateUnit(arguments.get(1));
            }
            else
            {
                sourceUnit = validateUnit(arguments.get(1));
                targetUnit = validateUnit(arguments.get(2));
            }

            long convertedValue = convertValue(value, sourceUnit, targetUnit);

            return UTF8Type.instance.fromString(convertedValue + " " + targetUnit.getSymbol());
        }

        private long convertValue(long valueToConvert, DataStorageUnit sourceUnit, DataStorageUnit targetUnit)
        {
            if (targetUnit == BYTES)
                return sourceUnit.toBytes(valueToConvert);
            else if (targetUnit == KIBIBYTES)
                return sourceUnit.toKibibytes(valueToConvert);
            else if (targetUnit == MEBIBYTES)
                return sourceUnit.toMebibytes(valueToConvert);
            else if (targetUnit == GIBIBYTES)
                return sourceUnit.toGibibytes(valueToConvert);
            else
                throw new InvalidRequestException("unsupported target unit " + targetUnit);
        }

        private DataStorageUnit validateUnit(String unitAsString)
        {
            try
            {
                return DataStorageUnit.fromSymbol(unitAsString);
            }
            catch (Exception ex)
            {
                throw new InvalidRequestException(ex.getMessage());
            }
        }

        public static FunctionFactory factory()
        {
            return new FunctionFactory(FUNCTION_NAME,
                                       fixed(INT, TINYINT, SMALLINT, BIGINT, VARINT, ASCII, TEXT),
                                       optional(fixed(ASCII)),
                                       optional(fixed(ASCII)))
            {
                @Override
                protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
                {
                    if (argTypes.isEmpty() || argTypes.size() > 3)
                        throw invalidNumberOfArgumentsException();

                    return new ToHumanSizeFct(argTypes.toArray(new AbstractType<?>[0]));
                }
            };
        }
    }

    private static long getValue(Arguments arguments)
    {
        Optional<String> maybeString = getAsString(arguments, 0);

        if (maybeString.isPresent())
        {
            try
            {
                return Long.parseLong(maybeString.get());
            }
            catch (Exception ex)
            {
                throw new InvalidRequestException("unable to convert string '" + maybeString.get() + "' to a value of type long");
            }
        }
        else
        {
            return arguments.getAsLong(0);
        }
    }

    private static Optional<String> getAsString(Arguments arguments, int i)
    {
        try
        {
            return Optional.ofNullable(arguments.get(i));
        }
        catch (Exception ex)
        {
            return Optional.empty();
        }
    }

    private ToHumanFcts()
    {
    }
}
