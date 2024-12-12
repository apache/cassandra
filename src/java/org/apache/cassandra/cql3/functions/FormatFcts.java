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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DataStorageSpec.DataStorageUnit;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
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

public class FormatFcts
{
    private static final DecimalFormat decimalFormat;

    static
    {
        decimalFormat = new DecimalFormat("#.##");
        decimalFormat.setRoundingMode(RoundingMode.HALF_UP);
    }

    /**
     * Formats a double value to a string with two decimal places.
     * <p>
     * Supported column types on which this function is possible to be applied:
     * <pre>DOUBLE</pre>
     */
    public static String format(double value)
    {
        return decimalFormat.format(value);
    }

    public static void addFunctionsTo(NativeFunctions functions)
    {
        functions.add(FormatBytesFct.factory());
        functions.add(FormatTimeFct.factory());
    }

    private static long validateAndGetValue(Arguments arguments)
    {
        if (arguments.containsNulls())
            throw new InvalidRequestException("none of the arguments may be null");

        long value = getValue(arguments);

        if (value < 0)
            throw new InvalidRequestException("value must be non-negative");

        return value;
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

    private FormatFcts()
    {
    }

    /**
     * Converts numeric value in a column to a value of specified unit.
     * <p>
     * If the function call contains just one argument - value to convert - then it will be
     * looked at as the value is of unit 'ms' and it will be converted to a value of a unit which is closest to it.
     * The result will be rounded to two decimal places.
     * E.g. If a value is (20 * 1000 + 250) then the unit will be in seconds and converted value will be 20.25.
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
     * format_time(val)
     * format_time(val, 'm') = format_time(val, 'ms', 'm')
     * format_time(val, 's', 'm')
     * format_time(val, 's', 'h')
     * format_time(val, 's', 'd')
     * format_time(val, 's') = format_time(val, 'ms', 's')
     * format_time(val, 'h') = format_time(val, 'ms', 'h')
     * </pre>
     * <p>
     * It is possible to convert values of a bigger unit to values of a smaller unit, e.g. this is possible:
     *
     * <pre>
     * format_time(val, 'm', 's')
     * </pre>
     * <p>
     * Values can be max of Double.MAX_VALUE, If the conversion produces overflown value, Double.MAX_VALUE will be returned.
     * <p>
     * Supported units are: d, h, m, s, ms, us, µs, ns
     * <p>
     * Supported column types on which this function is possible to be applied:
     * <pre>INT, TINYINT, SMALLINT, BIGINT, VARINT, ASCII, TEXT</pre>
     * For ASCII and TEXT types, text of such column has to be a non-negative number.
     * <p>
     * The conversion of negative values is not supported.
     */
    public static class FormatTimeFct extends NativeScalarFunction
    {
        private static final String FUNCTION_NAME = "format_time";

        private static final String[] UNITS = { "d", "h", "m", "s" };
        private static final long[] CONVERSION_FACTORS = { 86400000, 3600000, 60000, 1000 }; // Milliseconds in a day, hour, minute, second

        private FormatTimeFct(AbstractType<?>... argsTypes)
        {
            super(FUNCTION_NAME, UTF8Type.instance, argsTypes);
        }

        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            if (arguments.get(0) == null)
                return null;

            long value = validateAndGetValue(arguments);

            if (arguments.size() == 1)
            {
                Pair<Double, String> convertedValue = convertValue(value);
                return UTF8Type.instance.fromString(format(convertedValue.left) + ' ' + convertedValue.right);
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

            double convertedValue = convertValue(value, sourceUnit, targetUnit);
            return UTF8Type.instance.fromString(format(convertedValue) + ' ' + targetUnitAsString);
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

        private Pair<Double, String> convertValue(long valueToConvert)
        {
            for (int i = 0; i < CONVERSION_FACTORS.length; i++)
            {
                if (valueToConvert >= CONVERSION_FACTORS[i])
                {
                    double convertedValue = (double) valueToConvert / CONVERSION_FACTORS[i];
                    return Pair.create(convertedValue, UNITS[i]);
                }
            }
            return Pair.create((double) valueToConvert, "ms");
        }

        private Double convertValue(long valueToConvert, TimeUnit sourceUnit, TimeUnit targetUnit)
        {
            try
            {
                double conversionFactor = getConversionFactor(sourceUnit, targetUnit);
                return valueToConvert * conversionFactor;
            }
            catch (ArithmeticException ex)
            {
                return Double.MAX_VALUE;
            }
        }

        private static double getConversionFactor(TimeUnit sourceUnit, TimeUnit targetUnit)
        {
            // Define conversion factors between units
            double nanosPerSourceUnit = getNanosPerUnit(sourceUnit);
            double nanosPerTargetUnit = getNanosPerUnit(targetUnit);

            // Calculate the conversion factor
            return nanosPerSourceUnit / nanosPerTargetUnit;
        }

        private static double getNanosPerUnit(TimeUnit unit)
        {
            switch (unit)
            {
                case NANOSECONDS:
                    return 1.0;
                case MICROSECONDS:
                    return 1_000.0;
                case MILLISECONDS:
                    return 1_000_000.0;
                case SECONDS:
                    return 1_000_000_000.0;
                case MINUTES:
                    return 60.0 * 1_000_000_000.0;
                case HOURS:
                    return 3600.0 * 1_000_000_000.0;
                case DAYS:
                    return 86400.0 * 1_000_000_000.0;
                default:
                    throw new IllegalArgumentException("Unsupported time unit: " + unit);
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

                    return new FormatTimeFct(argTypes.toArray(new AbstractType<?>[0]));
                }
            };
        }
    }

    /**
     * Converts numeric value in a column to a size value of specified unit.
     * <p>
     * If the function call contains just one argument - value to convert - then it will be
     * looked at as the value is of unit 'B' and it will be converted to a value of a unit which is closest to it.
     * The result will be rounded to two decimal places.
     * E.g. If a value is (100 * 1024 + 150) then the unit will be in KiB and converted value will be 100.15.
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
     * format_bytes(val) = format_bytes(val, 'B', 'MiB')
     * format_bytes(val, 'B', 'MiB')
     * format_bytes(val, 'B', 'GiB')
     * format_bytes(val, 'KiB', 'GiB')
     * format_bytes(val, 'MiB') = format_bytes(val, 'B', 'MiB')
     * format_bytes(val, 'GiB') = format_bytes(val, 'B', 'GiB')
     * </pre>
     * <p>
     * It is possible to convert values of a bigger unit to values of a smaller unit, e.g. this is possible:
     *
     * <pre>
     * format_bytes(val, 'GiB', 'B')
     * </pre>
     * <p>
     * Values can be max of Long.MAX_VALUE, If the conversion produces overflown value, Long.MAX_VALUE will be returned.
     * Note that the actual return value will be 9223372036854776000 due to the limitations of double precision.
     * <p>
     * Supported units are: B, KiB, MiB, GiB
     * <p>
     * Supported column types on which this function is possible to be applied:
     * <pre>INT, TINYINT, SMALLINT, BIGINT, VARINT, ASCII, TEXT</pre>
     * For ASCII and TEXT types, text of such column has to be a non-negative number.
     * <p>
     * <p>
     * The conversion of negative values is not supported.
     */
    public static class FormatBytesFct extends NativeScalarFunction
    {
        private static final String FUNCTION_NAME = "format_bytes";

        private FormatBytesFct(AbstractType<?>... argsTypes)
        {
            super(FUNCTION_NAME, UTF8Type.instance, argsTypes);
        }

        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            if (arguments.get(0) == null)
                return null;

            long value = validateAndGetValue(arguments);

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

            double convertedValue = convertValue(value, sourceUnit, targetUnit);
            String convertedValueAsString = format(convertedValue);

            return UTF8Type.instance.fromString(convertedValueAsString + ' ' + targetUnit.getSymbol());
        }

        private double convertValue(long valueToConvert, DataStorageUnit sourceUnit, DataStorageUnit targetUnit)
        {
            switch (targetUnit)
            {
                case BYTES:
                    return sourceUnit.toBytesDouble(valueToConvert);
                case KIBIBYTES:
                    return sourceUnit.toKibibytesDouble(valueToConvert);
                case MEBIBYTES:
                    return sourceUnit.toMebibytesDouble(valueToConvert);
                case GIBIBYTES:
                    return sourceUnit.toGibibytesDouble(valueToConvert);
                default:
                    throw new InvalidRequestException("unsupported target unit " + targetUnit);
            }
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

                    return new FormatBytesFct(argTypes.toArray(new AbstractType<?>[0]));
                }
            };
        }
    }
}