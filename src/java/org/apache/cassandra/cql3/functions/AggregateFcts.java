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
import org.apache.cassandra.db.marshal.*;

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

                        public ByteBuffer compute(int protocolVersion)
                        {
                            return ((LongType) returnType()).decompose(count);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
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

                        public ByteBuffer compute(int protocolVersion)
                        {
                            return ((DecimalType) returnType()).decompose(sum);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            BigDecimal number = ((BigDecimal) argTypes().get(0).compose(value));
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
                        private BigDecimal sum = BigDecimal.ZERO;

                        private int count;

                        public void reset()
                        {
                            count = 0;
                            sum = BigDecimal.ZERO;
                        }

                        public ByteBuffer compute(int protocolVersion)
                        {
                            if (count == 0)
                                return ((DecimalType) returnType()).decompose(BigDecimal.ZERO);

                            return ((DecimalType) returnType()).decompose(sum.divide(BigDecimal.valueOf(count)));
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            count++;
                            BigDecimal number = ((BigDecimal) argTypes().get(0).compose(value));
                            sum = sum.add(number);
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

                        public ByteBuffer compute(int protocolVersion)
                        {
                            return ((IntegerType) returnType()).decompose(sum);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            BigInteger number = ((BigInteger) argTypes().get(0).compose(value));
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

                        public ByteBuffer compute(int protocolVersion)
                        {
                            if (count == 0)
                                return ((IntegerType) returnType()).decompose(BigInteger.ZERO);

                            return ((IntegerType) returnType()).decompose(sum.divide(BigInteger.valueOf(count)));
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
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
     * The SUM function for int32 values.
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

                        public ByteBuffer compute(int protocolVersion)
                        {
                            return ((ByteType) returnType()).decompose(sum);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
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
     * AVG function for int32 values.
     */
    public static final AggregateFunction avgFunctionForByte =
            new NativeAggregateFunction("avg", ByteType.instance, ByteType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private byte sum;

                        private int count;

                        public void reset()
                        {
                            count = 0;
                            sum = 0;
                        }

                        public ByteBuffer compute(int protocolVersion)
                        {
                            int avg = count == 0 ? 0 : sum / count;

                            return ((ByteType) returnType()).decompose((byte) avg);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            count++;
                            Number number = ((Number) argTypes().get(0).compose(value));
                            sum += number.byteValue();
                        }
                    };
                }
            };

    /**
     * The SUM function for int32 values.
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

                        public ByteBuffer compute(int protocolVersion)
                        {
                            return ((ShortType) returnType()).decompose(sum);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
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
     * AVG function for int32 values.
     */
    public static final AggregateFunction avgFunctionForShort =
            new NativeAggregateFunction("avg", ShortType.instance, ShortType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private short sum;

                        private int count;

                        public void reset()
                        {
                            count = 0;
                            sum = 0;
                        }

                        public ByteBuffer compute(int protocolVersion)
                        {
                            int avg = count == 0 ? 0 : sum / count;

                            return ((ShortType) returnType()).decompose((short) avg);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            count++;
                            Number number = ((Number) argTypes().get(0).compose(value));
                            sum += number.shortValue();
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

                        public ByteBuffer compute(int protocolVersion)
                        {
                            return ((Int32Type) returnType()).decompose(sum);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
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
                    return new Aggregate()
                    {
                        private int sum;

                        private int count;

                        public void reset()
                        {
                            count = 0;
                            sum = 0;
                        }

                        public ByteBuffer compute(int protocolVersion)
                        {
                            int avg = count == 0 ? 0 : sum / count;

                            return ((Int32Type) returnType()).decompose(avg);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            count++;
                            Number number = ((Number) argTypes().get(0).compose(value));
                            sum += number.intValue();
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
                    return new LongAvgAggregate();
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
                    return new Aggregate()
                    {
                        private float sum;

                        public void reset()
                        {
                            sum = 0;
                        }

                        public ByteBuffer compute(int protocolVersion)
                        {
                            return ((FloatType) returnType()).decompose(sum);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            Number number = ((Number) argTypes().get(0).compose(value));
                            sum += number.floatValue();
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
                    return new Aggregate()
                    {
                        private float sum;

                        private int count;

                        public void reset()
                        {
                            count = 0;
                            sum = 0;
                        }

                        public ByteBuffer compute(int protocolVersion)
                        {
                            float avg = count == 0 ? 0 : sum / count;

                            return ((FloatType) returnType()).decompose(avg);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            count++;
                            Number number = ((Number) argTypes().get(0).compose(value));
                            sum += number.floatValue();
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
                    return new Aggregate()
                    {
                        private double sum;

                        public void reset()
                        {
                            sum = 0;
                        }

                        public ByteBuffer compute(int protocolVersion)
                        {
                            return ((DoubleType) returnType()).decompose(sum);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            Number number = ((Number) argTypes().get(0).compose(value));
                            sum += number.doubleValue();
                        }
                    };
                }
            };

    /**
     * AVG function for double values.
     */
    public static final AggregateFunction avgFunctionForDouble =
            new NativeAggregateFunction("avg", DoubleType.instance, DoubleType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private double sum;

                        private int count;

                        public void reset()
                        {
                            count = 0;
                            sum = 0;
                        }

                        public ByteBuffer compute(int protocolVersion)
                        {
                            double avg = count == 0 ? 0 : sum / count;

                            return ((DoubleType) returnType()).decompose(avg);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            count++;
                            Number number = ((Number) argTypes().get(0).compose(value));
                            sum += number.doubleValue();
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
            return new LongAvgAggregate();
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

                public ByteBuffer compute(int protocolVersion)
                {
                    return min != null ? LongType.instance.decompose(min) : null;
                }

                public void addInput(int protocolVersion, List<ByteBuffer> values)
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
     * AVG function for counter column values.
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

                public ByteBuffer compute(int protocolVersion)
                {
                    return max != null ? LongType.instance.decompose(max) : null;
                }

                public void addInput(int protocolVersion, List<ByteBuffer> values)
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

                    public ByteBuffer compute(int protocolVersion)
                    {
                        return max;
                    }

                    public void addInput(int protocolVersion, List<ByteBuffer> values)
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

                    public ByteBuffer compute(int protocolVersion)
                    {
                        return min;
                    }

                    public void addInput(int protocolVersion, List<ByteBuffer> values)
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

                    public ByteBuffer compute(int protocolVersion)
                    {
                        return ((LongType) returnType()).decompose(count);
                    }

                    public void addInput(int protocolVersion, List<ByteBuffer> values)
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

        public ByteBuffer compute(int protocolVersion)
        {
            return LongType.instance.decompose(sum);
        }

        public void addInput(int protocolVersion, List<ByteBuffer> values)
        {
            ByteBuffer value = values.get(0);

            if (value == null)
                return;

            Number number = LongType.instance.compose(value);
            sum += number.longValue();
        }
    }

    private static class LongAvgAggregate implements AggregateFunction.Aggregate
    {
        private long sum;

        private int count;

        public void reset()
        {
            count = 0;
            sum = 0;
        }

        public ByteBuffer compute(int protocolVersion)
        {
            long avg = count == 0 ? 0 : sum / count;

            return LongType.instance.decompose(avg);
        }

        public void addInput(int protocolVersion, List<ByteBuffer> values)
        {
            ByteBuffer value = values.get(0);

            if (value == null)
                return;

            count++;
            Number number = LongType.instance.compose(value);
            sum += number.longValue();
        }
    }
}
