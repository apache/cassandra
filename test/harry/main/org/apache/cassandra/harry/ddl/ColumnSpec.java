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

package org.apache.cassandra.harry.ddl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.cassandra.harry.gen.Bijections;
import org.apache.cassandra.harry.gen.StringBijection;

public class ColumnSpec<T>
{
    public final String name;
    public final DataType<T> type;
    public final Kind kind;
    int columnIndex;

    public ColumnSpec(String name,
                      DataType<T> type,
                      Kind kind)
    {
        this.name = name;
        this.type = type;
        this.kind = kind;
    }


    public ColumnSpec<T> override(Bijections.Bijection<T> override)
    {
        return new ColumnSpec<>(name,
                                new DataType<>(type.cqlName) {
                                    @Override
                                    public int compareLexicographically(long l, long r)
                                    {
                                        return type.compareLexicographically(l, r);
                                    }

                                    @Override
                                    public boolean isReversed()
                                    {
                                        return type.isReversed();
                                    }

                                    @Override
                                    public Bijections.Bijection<T> generator()
                                    {
                                        return override;
                                    }
                                },
                                kind);
    }


    void setColumnIndex(int idx)
    {
        this.columnIndex = idx;
    }

    public int getColumnIndex()
    {
        return columnIndex;
    }

    public String toCQL()
    {
        return String.format("%s %s%s",
                             name,
                             type.toString(),
                             kind == Kind.STATIC ? " static" : "");
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnSpec<?> that = (ColumnSpec<?>) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(type.cqlName, that.type.cqlName) &&
               kind == that.kind;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type.cqlName, kind);
    }

    public String name()
    {
        return name;
    }

    public boolean isReversed()
    {
        return type.isReversed();
    }

    public String toString()
    {
        return name + '(' + type.toString() + ")";
    }

    public Bijections.Bijection<T> generator()
    {
        return type.generator();
    }

    public T inflate(long current)
    {
        return type.generator().inflate(current);
    }

    public long deflate(T value)
    {
        return type.generator().deflate(value);
    }

    public static ColumnSpec<?> pk(String name, DataType<?> type)
    {
        return new ColumnSpec<>(name, type, Kind.PARTITION_KEY);
    }

    @SuppressWarnings("unchecked")
    public static ColumnSpec<?> ck(String name, DataType<?> type, boolean isReversed)
    {
        return new ColumnSpec(name, isReversed ? ReversedType.getInstance(type) : type, Kind.CLUSTERING);
    }

    @SuppressWarnings("unchecked")
    public static ColumnSpec<?> ck(String name, DataType<?> type)
    {
        return new ColumnSpec(name, type, Kind.CLUSTERING);
    }

    public static ColumnSpec<?> regularColumn(String name, DataType<?> type)
    {
        return new ColumnSpec<>(name, type, Kind.REGULAR);
    }

    public static ColumnSpec<?> staticColumn(String name, DataType<?> type)
    {
        return new ColumnSpec<>(name, type, Kind.STATIC);
    }

    public enum Kind
    {
        CLUSTERING, REGULAR, STATIC, PARTITION_KEY
    }

    public static abstract class DataType<T>
    {
        protected final String cqlName;

        protected DataType(String cqlName)
        {
            this.cqlName = cqlName;
        }

        public boolean isReversed()
        {
            return false;
        }

        /**
         * Cassandra uses lexicographical oder for resolving timestamp ties
         */
        public int compareLexicographically(long l, long r)
        {
            for (int i = Long.BYTES - 1; i >= 0; i--)
            {
                int cmp = Integer.compare((int) ((l >> (i * 8)) & 0xffL),
                                          (int) ((r >> (i * 8)) & 0xffL));
                if (cmp != 0)
                    return cmp;
            }
            return 0;
        }

        public abstract Bijections.Bijection<T> generator();

        public int maxSize()
        {
            return generator().byteSize();
        }

        public final String toString()
        {
            return cqlName;
        }

        public String nameForParser()
        {
            return cqlName;
        }

        public final boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DataType<?> dataType = (DataType<?>) o;
            return Objects.equals(cqlName, dataType.cqlName);
        }

        public final int hashCode()
        {
            return Objects.hash(cqlName);
        }
    }

    public static final DataType<Byte> int8Type = new DataType<Byte>("tinyint")
    {
        public Bijections.Bijection<Byte> generator()
        {
            return Bijections.INT8_GENERATOR;
        }
    };

    public static final DataType<Short> int16Type = new DataType<Short>("smallint")
    {
        public Bijections.Bijection<Short> generator()
        {
            return Bijections.INT16_GENERATOR;
        }
    };

    public static final DataType<Integer> int32Type = new DataType<Integer>("int")
    {
        public Bijections.Bijection<Integer> generator()
        {
            return Bijections.INT32_GENERATOR;
        }
    };

    public static final DataType<Long> int64Type = new DataType<Long>("bigint")
    {
        public Bijections.Bijection<Long> generator()
        {
            return Bijections.INT64_GENERATOR;
        }
    };

    public static final DataType<Boolean> booleanType = new DataType<Boolean>("boolean")
    {
        public Bijections.Bijection<Boolean> generator()
        {
            return Bijections.BOOLEAN_GENERATOR;
        }

        public int compareLexicographically(long l, long r)
        {
            throw new RuntimeException("Boolean does not support custom comparators");
        }
    };

    public static final DataType<Float> floatType = new DataType<Float>("float")
    {
        public Bijections.Bijection<Float> generator()
        {
            return Bijections.FLOAT_GENERATOR;
        }
    };

    public static final DataType<Double> doubleType = new DataType<Double>("double")
    {
        public Bijections.Bijection<Double> generator()
        {
            return Bijections.DOUBLE_GENERATOR;
        }
    };

    public static final DataType<String> asciiType = new DataType<String>("ascii")
    {
        private final Bijections.Bijection<String> gen = new StringBijection();

        public Bijections.Bijection<String> generator()
        {
            return gen;
        }

        public int compareLexicographically(long l, long r)
        {
            return Long.compare(l, r);
        }
    };

    public static final DataType<String> textType = new DataType<String>("text")
    {
        private final Bijections.Bijection<String> gen = new StringBijection();

        public Bijections.Bijection<String> generator()
        {
            return gen;
        }

        public int compareLexicographically(long l, long r)
        {
            return Long.compare(l, r);
        }
    };

    public static DataType<String> asciiType(int nibbleSize, int maxRandomBytes)
    {
        Bijections.Bijection<String> gen = new StringBijection(nibbleSize, maxRandomBytes);

        return new DataType<String>("ascii")
        {
            public Bijections.Bijection<String> generator()
            {
                return gen;
            }

            public int compareLexicographically(long l, long r)
            {
                return Long.compare(l, r);
            }

            public String nameForParser()
            {
                return String.format("%s(%d,%d)",
                                     super.nameForParser(),
                                     nibbleSize,
                                     maxRandomBytes);
            }
        };
    }

    public static final DataType<UUID> uuidType = new DataType<UUID>("uuid")
    {
        public Bijections.Bijection<UUID> generator()
        {
            return Bijections.UUID_GENERATOR;
        }

        public int compareLexicographically(long l, long r)
        {
            throw new RuntimeException("UUID does not support custom comparators");
        }
    };

    public static final DataType<UUID> timeUuidType = new DataType<UUID>("timeuuid")
    {
        public Bijections.Bijection<UUID> generator()
        {
            return Bijections.TIME_UUID_GENERATOR;
        }

        public int compareLexicographically(long l, long r)
        {
            throw new RuntimeException("UUID does not support custom comparators");
        }
    };

    public static final DataType<Date> timestampType = new DataType<Date>("timestamp")
    {
        public Bijections.Bijection<Date> generator()
        {
            return Bijections.TIMESTAMP_GENERATOR;
        }

        public int compareLexicographically(long l, long r)
        {
            throw new RuntimeException("Date does not support custom comparators");
        }
    };

    public static final Collection<DataType<?>> DATA_TYPES = Collections.unmodifiableList(
    Arrays.asList(ColumnSpec.int8Type,
                  ColumnSpec.int16Type,
                  ColumnSpec.int32Type,
                  ColumnSpec.int64Type,
                  ColumnSpec.booleanType,
                  ColumnSpec.floatType,
                  ColumnSpec.doubleType,
                  ColumnSpec.asciiType,
                  ColumnSpec.textType,
                  ColumnSpec.uuidType,
                  ColumnSpec.timeUuidType,
                  ColumnSpec.timestampType));

    public static class ReversedType<T> extends DataType<T>
    {
        public static final Map<DataType<?>, ReversedType<?>> cache = new HashMap()
        {{
            put(int8Type, new ReversedType<>(int8Type));
            put(int16Type, new ReversedType<>(int16Type));
            put(int32Type, new ReversedType<>(int32Type));
            put(int64Type, new ReversedType<>(int64Type));
            put(booleanType, new ReversedType<>(booleanType));
            put(floatType, new ReversedType<>(floatType, new Bijections.ReverseFloatGenerator()));
            put(doubleType, new ReversedType<>(doubleType, new Bijections.ReverseDoubleGenerator()));
            put(asciiType, new ReversedType<>(asciiType));
            put(uuidType, new ReversedType<>(uuidType));
            put(timeUuidType, new ReversedType<>(timeUuidType));
        }};

        private final DataType<T> baseType;
        private final Bijections.Bijection<T> generator;

        public ReversedType(DataType<T> baseType)
        {
            super(baseType.cqlName);
            this.baseType = baseType;
            this.generator = new Bijections.ReverseBijection<>(baseType.generator());
        }

        public ReversedType(DataType<T> baseType, Bijections.Bijection<T> generator)
        {
            super(baseType.cqlName);
            this.baseType = baseType;
            this.generator = generator;
        }

        public boolean isReversed()
        {
            return true;
        }

        public Bijections.Bijection<T> generator()
        {
            return generator;
        }

        public int maxSize()
        {
            return baseType.maxSize();
        }

        public static <T> DataType<T> getInstance(DataType<T> type)
        {
            ReversedType<T> t = (ReversedType<T>) cache.get(type);
            if (t == null)
                t = new ReversedType<>(type);
            assert t.baseType == type : String.format("Type mismatch %s != %s", t.baseType, type);
            return t;
        }
    }
}