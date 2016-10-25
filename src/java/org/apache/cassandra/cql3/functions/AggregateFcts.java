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
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Factory methods for aggregate functions.
 */
public abstract class AggregateFcts
{
    public static Collection<AggregateFunction> all()
    {
        Collection<AggregateFunction> functions = new ArrayList<>();

        functions.add(countRowsFunction);

        // sum for primitives
        functions.add(sumFunctionForByte);
        functions.add(sumFunctionForShort);
        functions.add(sumFunctionForInt32);
        functions.add(sumFunctionForLong);
        functions.add(sumFunctionForFloat);
        functions.add(sumFunctionForDouble);
        functions.add(sumFunctionForDecimal);
        functions.add(sumFunctionForVarint);
        functions.add(sumFunctionForCounter);

        // avg for primitives
        functions.add(avgFunctionForByte);
        functions.add(avgFunctionForShort);
        functions.add(avgFunctionForInt32);
        functions.add(avgFunctionForLong);
        functions.add(avgFunctionForFloat);
        functions.add(avgFunctionForDouble);
        functions.add(avgFunctionForDecimal);
        functions.add(avgFunctionForVarint);
        functions.add(avgFunctionForCounter);

        // count, max, and min for all standard types
        for (CQL3Type type : CQL3Type.Native.values())
        {
            if (type != CQL3Type.Native.VARCHAR) // varchar and text both mapping to UTF8Type
            {
                functions.add(AggregateFcts.makeCountFunction(type.getType()));
                if (type != CQL3Type.Native.COUNTER)
                {
                    functions.add(AggregateFcts.makeMaxFunction(type.getType()));
                    functions.add(AggregateFcts.makeMinFunction(type.getType()));
                }
                else
                {
                    functions.add(AggregateFcts.maxFunctionForCounter);
                    functions.add(AggregateFcts.minFunctionForCounter);
                }
            }
        }

        return functions;
    }

    /**
     * The function used to count the number of rows of a result set. This function is called when COUNT(*) or COUNT(1)
     * is specified.
     */
    public static final AggregateFunction countRowsFunction =
            new NativeAggregateFunction("countRows", LongType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private long count;

                        public void reset()
                        {
                            count = 0;
                        }

                        public ByteBuffer compute(ProtocolVersion protocolVersion)
                        {
                            return LongType.instance.decompose(count);
                        }

                        public void addInput(ProtocolVersion protocolVersion, List<ByteBuffer> values)
                        {
                            count++;
                        }
                    };
                }

                @Override
                public String columnName(List<String> columnNames)
                {
                    return "count";
                }
            };

    /**
     * The SUM function for decimal values.
     */
    public static final AggregateFunction sumFunctionForDecimal =
            new NativeAggregateFunction("sum", DecimalType.instance, DecimalType.instance)
            {
                @Override
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private BigDecimal sum = BigDecimal.ZERO;

                        public void reset()
                        {
                            sum = BigDecimal.ZERO;
                        }

                        public ByteBuffer compute(ProtocolVersion protocolVersion)
                        {
                            return ((DecimalType) returnType()).decompose(sum);
                        }

                        public void addInput(ProtocolVersion protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            BigDecimal number = DecimalType.instance.compose(value);
                            sum = sum.add(number);
                        }
                    };
                }
            };

    /**
     * The AVG function for decimal values.
     */
    public static final AggregateFunction avgFunctionForDecimal =
            new NativeAggregateFunction("avg", DecimalType.instance, DecimalType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private BigDecimal avg = BigDecimal.ZERO;

                        private int count;

                        public void reset()
                        {
                            count = 0;
                            avg = BigDecimal.ZERO;
                        }

                        public ByteBuffer compute(ProtocolVersion protocolVersion)
                        {
                            return DecimalType.instance.decompose(avg);
                        }

                        public void addInput(ProtocolVersion protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            count++;
                            BigDecimal number = DecimalType.instance.compose(value);

                            // avg = avg + (value - sum) / count.
                            avg = avg.add(number.subtract(avg).divide(BigDecimal.valueOf(count), RoundingMode.HALF_EVEN));
                        }
                    };
                }
            };


    /**
     * The SUM function for varint values.
     */
    public static final AggregateFunction sumFunctionForVarint =
            new NativeAggregateFunction("sum", IntegerType.instance, IntegerType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private BigInteger sum = BigInteger.ZERO;

                        public void reset()
                        {
                            sum = BigInteger.ZERO;
                        }

                        public ByteBuffer compute(ProtocolVersion protocolVersion)
                        {
                            return ((IntegerType) returnType()).decompose(sum);
                        }

                        public void addInput(ProtocolVersion protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            BigInteger number = IntegerType.instance.compose(value);
                            sum = sum.add(number);
                        }
                    };
                }
            };

    /**
     * The AVG function for varint values.
     */
    public static final AggregateFunction avgFunctionForVarint =
            new NativeAggregateFunction("avg", IntegerType.instance, IntegerType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private BigInteger sum = BigInteger.ZERO;

                        private int count;

                        public void reset()
                        {
                            count = 0;
                            sum = BigInteger.ZERO;
                        }

                        public ByteBuffer compute(ProtocolVersion protocolVersion)
                        {
                            if (count == 0)
                                return IntegerType.instance.decompose(BigInteger.ZERO);

                            return IntegerType.instance.decompose(sum.divide(BigInteger.valueOf(count)));
                        }

                        public void addInput(ProtocolVersion protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            count++;
                            BigInteger number = ((BigInteger) argTypes().get(0).compose(value));
                            sum = sum.add(number);
                        }
                    };
                }
            };

    /**
     * The SUM function for byte values (tinyint).
     */
    public static final AggregateFunction sumFunctionForByte =
            new NativeAggregateFunction("sum", ByteType.instance, ByteType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private byte sum;

                        public void reset()
                        {
                            sum = 0;
                        }

                        public ByteBuffer compute(ProtocolVersion protocolVersion)
                        {
                            return ((ByteType) returnType()).decompose(sum);
                        }

                        public void addInput(ProtocolVersion protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            Number number = ((Number) argTypes().get(0).compose(value));
                            sum += number.byteValue();
                        }
                    };
                }
            };

    /**
     * AVG function for byte values (tinyint).
     */
    public static final AggregateFunction avgFunctionForByte =
            new NativeAggregateFunction("avg", ByteType.instance, ByteType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new AvgAggregate(ByteType.instance)
                    {
                        public ByteBuffer compute(ProtocolVersion protocolVersion) throws InvalidRequestException
                        {
                            return ByteType.instance.decompose((byte) computeInternal());
                        }
                    };
                }
            };

    /**
     * The SUM function for short values (smallint).
     */
    public static final AggregateFunction sumFunctionForShort =
            new NativeAggregateFunction("sum", ShortType.instance, ShortType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private short sum;

                        public void reset()
                        {
                            sum = 0;
                        }

                        public ByteBuffer compute(ProtocolVersion protocolVersion)
                        {
                            return ((ShortType) returnType()).decompose(sum);
                        }

                        public void addInput(ProtocolVersion protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            Number number = ((Number) argTypes().get(0).compose(value));
                            sum += number.shortValue();
                        }
                    };
                }
            };

    /**
     * AVG function for for short values (smallint).
     */
    public static final AggregateFunction avgFunctionForShort =
            new NativeAggregateFunction("avg", ShortType.instance, ShortType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new AvgAggregate(ShortType.instance)
                    {
                        public ByteBuffer compute(ProtocolVersion protocolVersion)
                        {
                            return ShortType.instance.decompose((short) computeInternal());
                        }
                    };
                }
            };

    /**
     * The SUM function for int32 values.
     */
    public static final AggregateFunction sumFunctionForInt32 =
            new NativeAggregateFunction("sum", Int32Type.instance, Int32Type.instance)
            {
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private int sum;

                        public void reset()
                        {
                            sum = 0;
                        }

                        public ByteBuffer compute(ProtocolVersion protocolVersion)
                        {
                            return ((Int32Type) returnType()).decompose(sum);
                        }

                        public void addInput(ProtocolVersion protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            Number number = ((Number) argTypes().get(0).compose(value));
                            sum += number.intValue();
                        }
                    };
                }
            };

    /**
     * AVG function for int32 values.
     */
    public static final AggregateFunction avgFunctionForInt32 =
            new NativeAggregateFunction("avg", Int32Type.instance, Int32Type.instance)
            {
                public Aggregate newAggregate()
                {
                    return new AvgAggregate(Int32Type.instance)
                    {
                        public ByteBuffer compute(ProtocolVersion protocolVersion)
                        {
                            return Int32Type.instance.decompose((int) computeInternal());
                        }
                    };
                }
            };

    /**
     * The SUM function for long values.
     */
    public static final AggregateFunction sumFunctionForLong =
            new NativeAggregateFunction("sum", LongType.instance, LongType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new LongSumAggregate();
                }
            };

    /**
     * AVG function for long values.
     */
    public static final AggregateFunction avgFunctionForLong =
            new NativeAggregateFunction("avg", LongType.instance, LongType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new AvgAggregate(LongType.instance)
                    {
                        public ByteBuffer compute(ProtocolVersion protocolVersion)
                        {
                            return LongType.instance.decompose(computeInternal());
                        }
                    };
                }
            };

    /**
     * The SUM function for float values.
     */
    public static final AggregateFunction sumFunctionForFloat =
            new NativeAggregateFunction("sum", FloatType.instance, FloatType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new FloatSumAggregate(FloatType.instance)
                    {
                        public ByteBuffer compute(ProtocolVersion protocolVersion) throws InvalidRequestException
                        {
                            return FloatType.instance.decompose((float) computeInternal());
                        }
                    };
                }
            };

    /**
     * AVG function for float values.
     */
    public static final AggregateFunction avgFunctionForFloat =
            new NativeAggregateFunction("avg", FloatType.instance, FloatType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new FloatAvgAggregate(FloatType.instance)
                    {
                        public ByteBuffer compute(ProtocolVersion protocolVersion) throws InvalidRequestException
                        {
                            return FloatType.instance.decompose((float) computeInternal());
                        }
                    };
                }
            };

    /**
     * The SUM function for double values.
     */
    public static final AggregateFunction sumFunctionForDouble =
            new NativeAggregateFunction("sum", DoubleType.instance, DoubleType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new FloatSumAggregate(DoubleType.instance)
                    {
                        public ByteBuffer compute(ProtocolVersion protocolVersion) throws InvalidRequestException
                        {
                            return DoubleType.instance.decompose(computeInternal());
                        }
                    };
                }
            };

    /**
     * Sum aggregate function for floating point numbers, using double arithmetics and
     * Kahan's algorithm to improve result precision.
     */
    private static abstract class FloatSumAggregate implements AggregateFunction.Aggregate
    {
        private double sum;
        private double compensation;
        private double simpleSum;

        private final AbstractType numberType;

        public FloatSumAggregate(AbstractType numberType)
        {
            this.numberType = numberType;
        }

        public void reset()
        {
            sum = 0;
            compensation = 0;
            simpleSum = 0;
        }

        public void addInput(ProtocolVersion protocolVersion, List<ByteBuffer> values)
        {
            ByteBuffer value = values.get(0);

            if (value == null)
                return;

            double number = ((Number) numberType.compose(value)).doubleValue();
            simpleSum += number;
            double tmp = number - compensation;
            double rounded = sum + tmp;
            compensation = (rounded - sum) - tmp;
            sum = rounded;
        }

        public double computeInternal()
        {
            // correctly compute final sum if it's NaN from consequently
            // adding same-signed infinite values.
            double tmp = sum + compensation;

            if (Double.isNaN(tmp) && Double.isInfinite(simpleSum))
                return simpleSum;
            else
                return tmp;
        }
    }

    /**
     * Average aggregate for floating point umbers, using double arithmetics and Kahan's algorithm
     * to calculate sum by default, switching to BigDecimal on sum overflow. Resulting number is
     * converted to corresponding representation by concrete implementations.
     */
    private static abstract class FloatAvgAggregate implements AggregateFunction.Aggregate
    {
        private double sum;
        private double compensation;
        private double simpleSum;

        private int count;

        private BigDecimal bigSum = null;
        private boolean overflow = false;

        private final AbstractType numberType;

        public FloatAvgAggregate(AbstractType numberType)
        {
            this.numberType = numberType;
        }

        public void reset()
        {
            sum = 0;
            compensation = 0;
            simpleSum = 0;

            count = 0;
            bigSum = null;
            overflow = false;
        }

        public double computeInternal()
        {
            if (count == 0)
                return 0d;

            if (overflow)
            {
                return bigSum.divide(BigDecimal.valueOf(count), RoundingMode.HALF_EVEN).doubleValue();
            }
            else
            {
                // correctly compute final sum if it's NaN from consequently
                // adding same-signed infinite values.
                double tmp = sum + compensation;
                if (Double.isNaN(tmp) && Double.isInfinite(simpleSum))
                    sum = simpleSum;
                else
                    sum = tmp;

                return sum / count;
            }
        }

        public void addInput(ProtocolVersion protocolVersion, List<ByteBuffer> values)
        {
            ByteBuffer value = values.get(0);

            if (value == null)
                return;

            count++;

            double number = ((Number) numberType.compose(value)).doubleValue();

            if (overflow)
            {
                bigSum = bigSum.add(BigDecimal.valueOf(number));
            }
            else
            {
                simpleSum += number;
                double prev = sum;
                double tmp = number - compensation;
                double rounded = sum + tmp;
                compensation = (rounded - sum) - tmp;
                sum = rounded;

                if (Double.isInfinite(sum) && !Double.isInfinite(number))
                {
                    overflow = true;
                    bigSum = BigDecimal.valueOf(prev).add(BigDecimal.valueOf(number));
                }
            }
        }
    }

    /**
     * AVG function for double values.
     */
    public static final AggregateFunction avgFunctionForDouble =
            new NativeAggregateFunction("avg", DoubleType.instance, DoubleType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new FloatAvgAggregate(DoubleType.instance)
                    {
                        public ByteBuffer compute(ProtocolVersion protocolVersion) throws InvalidRequestException
                        {
                            return DoubleType.instance.decompose(computeInternal());
                        }
                    };
                }
            };

    /**
     * The SUM function for counter column values.
     */
    public static final AggregateFunction sumFunctionForCounter =
    new NativeAggregateFunction("sum", CounterColumnType.instance, CounterColumnType.instance)
    {
        public Aggregate newAggregate()
        {
            return new LongSumAggregate();
        }
    };

    /**
     * AVG function for counter column values.
     */
    public static final AggregateFunction avgFunctionForCounter =
    new NativeAggregateFunction("avg", CounterColumnType.instance, CounterColumnType.instance)
    {
        public Aggregate newAggregate()
        {
            return new AvgAggregate(LongType.instance)
            {
                public ByteBuffer compute(ProtocolVersion protocolVersion) throws InvalidRequestException
                {
                    return CounterColumnType.instance.decompose(computeInternal());
                }
            };
        }
    };

    /**
     * The MIN function for counter column values.
     */
    public static final AggregateFunction minFunctionForCounter =
    new NativeAggregateFunction("min", CounterColumnType.instance, CounterColumnType.instance)
    {
        public Aggregate newAggregate()
        {
            return new Aggregate()
            {
                private Long min;

                public void reset()
                {
                    min = null;
                }

                public ByteBuffer compute(ProtocolVersion protocolVersion)
                {
                    return min != null ? LongType.instance.decompose(min) : null;
                }

                public void addInput(ProtocolVersion protocolVersion, List<ByteBuffer> values)
                {
                    ByteBuffer value = values.get(0);

                    if (value == null)
                        return;

                    long lval = LongType.instance.compose(value);

                    if (min == null || lval < min)
                        min = lval;
                }
            };
        }
    };

    /**
     * MAX function for counter column values.
     */
    public static final AggregateFunction maxFunctionForCounter =
    new NativeAggregateFunction("max", CounterColumnType.instance, CounterColumnType.instance)
    {
        public Aggregate newAggregate()
        {
            return new Aggregate()
            {
                private Long max;

                public void reset()
                {
                    max = null;
                }

                public ByteBuffer compute(ProtocolVersion protocolVersion)
                {
                    return max != null ? LongType.instance.decompose(max) : null;
                }

                public void addInput(ProtocolVersion protocolVersion, List<ByteBuffer> values)
                {
                    ByteBuffer value = values.get(0);

                    if (value == null)
                        return;

                    long lval = LongType.instance.compose(value);

                    if (max == null || lval > max)
                        max = lval;
                }
            };
        }
    };

    /**
     * Creates a MAX function for the specified type.
     *
     * @param inputType the function input and output type
     * @return a MAX function for the specified type.
     */
    public static AggregateFunction makeMaxFunction(final AbstractType<?> inputType)
    {
        return new NativeAggregateFunction("max", inputType, inputType)
        {
            public Aggregate newAggregate()
            {
                return new Aggregate()
                {
                    private ByteBuffer max;

                    public void reset()
                    {
                        max = null;
                    }

                    public ByteBuffer compute(ProtocolVersion protocolVersion)
                    {
                        return max;
                    }

                    public void addInput(ProtocolVersion protocolVersion, List<ByteBuffer> values)
                    {
                        ByteBuffer value = values.get(0);

                        if (value == null)
                            return;

                        if (max == null || returnType().compare(max, value) < 0)
                            max = value;
                    }
                };
            }
        };
    }

    /**
     * Creates a MIN function for the specified type.
     *
     * @param inputType the function input and output type
     * @return a MIN function for the specified type.
     */
    public static AggregateFunction makeMinFunction(final AbstractType<?> inputType)
    {
        return new NativeAggregateFunction("min", inputType, inputType)
        {
            public Aggregate newAggregate()
            {
                return new Aggregate()
                {
                    private ByteBuffer min;

                    public void reset()
                    {
                        min = null;
                    }

                    public ByteBuffer compute(ProtocolVersion protocolVersion)
                    {
                        return min;
                    }

                    public void addInput(ProtocolVersion protocolVersion, List<ByteBuffer> values)
                    {
                        ByteBuffer value = values.get(0);

                        if (value == null)
                            return;

                        if (min == null || returnType().compare(min, value) > 0)
                            min = value;
                    }
                };
            }
        };
    }

    /**
     * Creates a COUNT function for the specified type.
     *
     * @param inputType the function input type
     * @return a COUNT function for the specified type.
     */
    public static AggregateFunction makeCountFunction(AbstractType<?> inputType)
    {
        return new NativeAggregateFunction("count", LongType.instance, inputType)
        {
            public Aggregate newAggregate()
            {
                return new Aggregate()
                {
                    private long count;

                    public void reset()
                    {
                        count = 0;
                    }

                    public ByteBuffer compute(ProtocolVersion protocolVersion)
                    {
                        return ((LongType) returnType()).decompose(count);
                    }

                    public void addInput(ProtocolVersion protocolVersion, List<ByteBuffer> values)
                    {
                        ByteBuffer value = values.get(0);

                        if (value == null)
                            return;

                        count++;
                    }
                };
            }
        };
    }

    private static class LongSumAggregate implements AggregateFunction.Aggregate
    {
        private long sum;

        public void reset()
        {
            sum = 0;
        }

        public ByteBuffer compute(ProtocolVersion protocolVersion)
        {
            return LongType.instance.decompose(sum);
        }

        public void addInput(ProtocolVersion protocolVersion, List<ByteBuffer> values)
        {
            ByteBuffer value = values.get(0);

            if (value == null)
                return;

            Number number = LongType.instance.compose(value);
            sum += number.longValue();
        }
    }

    /**
     * Average aggregate class, collecting the sum using long arithmetics, falling back
     * to BigInteger on long overflow. Resulting number is converted to corresponding
     * representation by concrete implementations.
     */
    private static abstract class AvgAggregate implements AggregateFunction.Aggregate
    {
        private long sum;
        private int count;
        private BigInteger bigSum = null;
        private boolean overflow = false;

        private final AbstractType numberType;

        public AvgAggregate(AbstractType type)
        {
            this.numberType = type;
        }

        public void reset()
        {
            count = 0;
            sum = 0L;
            overflow = false;
            bigSum = null;
        }

        long computeInternal()
        {
            if (overflow)
            {
                return bigSum.divide(BigInteger.valueOf(count)).longValue();
            }
            else
            {
                return count == 0 ? 0 : (sum / count);
            }
        }

        public void addInput(ProtocolVersion protocolVersion, List<ByteBuffer> values)
        {
            ByteBuffer value = values.get(0);

            if (value == null)
                return;

            count++;
            long number = ((Number) numberType.compose(value)).longValue();
            if (overflow)
            {
                bigSum = bigSum.add(BigInteger.valueOf(number));
            }
            else
            {
                long prev = sum;
                sum += number;

                if (((prev ^ sum) & (number ^ sum)) < 0)
                {
                    overflow = true;
                    bigSum = BigInteger.valueOf(prev).add(BigInteger.valueOf(number));
                }
            }
        }
    }
}
