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

package org.apache.cassandra.harry;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import accord.utils.Invariants;
import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.TypeAdapters;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

// TODO: counters
// TODO: UDTs
// TODO: collections: frozen/unfrozen
// TODO: empty / 0 / min / max values if present
public class ColumnSpec<T>
{
    public final String name;
    public final DataType<T> type;
    public final Generator<T> gen;
    public final Kind kind;

    public ColumnSpec(String name,
                      DataType<T> type,
                      Generator<T> gen,
                      Kind kind)
    {

        this.name = name;
        this.type = Invariants.nonNull(type);
        this.gen = Invariants.nonNull(gen);
        this.kind = kind;
    }

    public String toCQL()
    {
        return String.format("%s %s%s",
                             Symbol.maybeQuote(name),
                             type,
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

    public Generator<T> gen()
    {
        return gen;
    }

    public static <T> ColumnSpec<T> pk(String name, DataType<T> type, Generator<T> gen)
    {
        return new ColumnSpec<>(name, type, gen, Kind.PARTITION_KEY);
    }

    public static <T> ColumnSpec<T> pk(String name, DataType<T> type)
    {
        return new ColumnSpec<>(name, type, (Generator<T>) TypeAdapters.forValues(type.asServerType()), Kind.PARTITION_KEY);
    }

    @SuppressWarnings("unchecked")
    public static <T> ColumnSpec<T> ck(String name, DataType<T> type, Generator<T> gen, boolean isReversed)
    {
        return new ColumnSpec(name, isReversed ? ReversedType.getInstance(type) : type, gen, Kind.CLUSTERING);
    }

    public static <T> ColumnSpec<T> ck(String name, DataType<T> type)
    {
        return ck(name, type, false);
    }

    public static <T> ColumnSpec<T> ck(String name, DataType<T> type, boolean isReversed)
    {
        return new ColumnSpec(name, isReversed ? ReversedType.getInstance(type) : type,
                              TypeAdapters.forValues(type.asServerType()),
                              Kind.CLUSTERING);
    }


    public static <T> ColumnSpec<T> regularColumn(String name, DataType<T> type, Generator<T> gen)
    {
        return new ColumnSpec<>(name, type, gen, Kind.REGULAR);
    }

    public static <T> ColumnSpec<T> regularColumn(String name, DataType<T> type)
    {
        return new ColumnSpec(name, type, TypeAdapters.forValues(type.asServerType()), Kind.REGULAR);
    }

    public static <T> ColumnSpec<T> staticColumn(String name, DataType<T> type, Generator<T> gen)
    {
        return new ColumnSpec<>(name, type, gen, Kind.STATIC);
    }

    public static <T> ColumnSpec<T> staticColumn(String name, DataType<T> type)
    {
        return new ColumnSpec(name, type, TypeAdapters.forValues(type.asServerType()), Kind.STATIC);
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

        public abstract /* unsigned */ long typeEntropy();

        public boolean isReversed()
        {
            return false;
        }

        public abstract AbstractType<?> asServerType();

        public final String toString()
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

        public abstract Comparator<T> comparator();
    }

    public static abstract class ComparableDataType<T extends Comparable<? super T>> extends DataType<T>
    {
        protected ComparableDataType(String cqlName)
        {
            super(cqlName);
        }

        @Override
        public Comparator<T> comparator()
        {
            return Comparable::compareTo;
        }
    }

    public static final DataType<Byte> int8Type = new ComparableDataType<>("tinyint")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << (8 * Byte.BYTES);
        }

        @Override
        public AbstractType<?> asServerType()
        {
            return ByteType.instance;
        }
    };

    public static final DataType<Short> int16Type = new ComparableDataType<>("smallint")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << Byte.SIZE;
        }

        @Override
        public AbstractType<?> asServerType()
        {
            return ShortType.instance;
        }
    };

    public static final DataType<Integer> int32Type = new ComparableDataType<>("int")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << Integer.SIZE;
        }

        @Override
        public AbstractType<?> asServerType()
        {
            return Int32Type.instance;
        }
    };

    public static final DataType<Long> int64Type = new ComparableDataType<Long>("bigint")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << (8 * Long.BYTES - 1);
        }

        @Override
        public AbstractType<?> asServerType()
        {
            return LongType.instance;
        }
    };

    public static final DataType<Boolean> booleanType = new ComparableDataType<Boolean>("boolean")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 2;
        }

        @Override
        public AbstractType<?> asServerType()
        {
            return BooleanType.instance;
        }
    };

    public static final DataType<Float> floatType = new ComparableDataType<Float>("float")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << (4 * Float.BYTES);
        }

        @Override
        public AbstractType<?> asServerType()
        {
            return FloatType.instance;
        }
    };

    public static final DataType<Double> doubleType = new ComparableDataType<Double>("double")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << 63;
        }

        @Override
        public AbstractType<?> asServerType()
        {
            return DoubleType.instance;
        }
    };

    public static final DataType<ByteBuffer> blobType = new DataType<>("blob")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << 63;
        }

        @Override
        public AbstractType<?> asServerType()
        {
            return BytesType.instance;
        }

        public Comparator<ByteBuffer> comparator()
        {
            return ByteBufferUtil::compareUnsigned;
        }
    };

    public static final DataType<String> asciiType = new ComparableDataType<>("ascii")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << 63;
        }

        @Override
        public AbstractType<?> asServerType()
        {
            return AsciiType.instance;
        }
    };

    // utf8
    public static final DataType<String> textType = new DataType<>("text")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << 63;
        }

        @Override
        public AbstractType<?> asServerType()
        {
            return UTF8Type.instance;
        }

        @Override
        public Comparator<String> comparator()
        {
            return (o1, o2) -> ByteArrayUtil.compareUnsigned(o1.getBytes(), o2.getBytes());
        }
    };

    public static final DataType<UUID> uuidType = new DataType<>("uuid")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << 63;
        }

        @Override
        public AbstractType<?> asServerType()
        {
            return UUIDType.instance;
        }

        public Comparator<UUID> comparator()
        {
            // TODO: avoid serialization to match C* order
            return (o1, o2) -> UUIDType.instance.compare(UUIDType.instance.decompose(o1),
                                                         UUIDType.instance.decompose(o2));
        }
    };

    public static final DataType<TimeUUID> timeUuidType = new ComparableDataType<>("timeuuid")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << 63;
        }

        @Override
        public AbstractType<?> asServerType()
        {
            return TimeUUIDType.instance;
        }
    };

    public static final DataType<Date> timestampType = new ComparableDataType<>("timestamp")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << 63;
        }

        @Override
        public AbstractType<?> asServerType()
        {
            return TimestampType.instance;
        }
    };

    public static final DataType<BigInteger> varintType = new ComparableDataType<>("varint")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << 63;
        }

        @Override
        public AbstractType<?> asServerType()
        {
            return IntegerType.instance;
        }
    };

    public static final DataType<Long> timeType = new ComparableDataType<>("time")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << 63;
        }

        @Override
        public AbstractType<?> asServerType()
        {
            return TimeType.instance;
        }
    };

    public static final DataType<BigDecimal> decimalType = new ComparableDataType<>("decimal")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << 63;
        }

        @Override
        public AbstractType<?> asServerType()
        {
            return DecimalType.instance;
        }
    };

    public static final DataType<InetAddress> inetType = new DataType<>("inet")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << 63;
        }

        @Override
        public AbstractType<?> asServerType()
        {
            return InetAddressType.instance;
        }

        @Override
        public Comparator<InetAddress> comparator()
        {
            return (o1, o2) -> {
                byte[] b1 = o1.getAddress();
                byte[] b2 = o2.getAddress();
                return ByteArrayUtil.compareUnsigned(b1, b2);
            };
        }
    };

    public static final List<DataType<?>> TYPES;

    static
    {
        List<DataType<?>> types = new ArrayList<>()
        {{
            add(int8Type);
            add(int16Type);
            add(int32Type);
            add(int64Type);
            add(floatType);
            add(doubleType);
            // TODO: SAI tests seem to fail these types
            // add(booleanType);
            // add(inetType);
            // add(varintType);
            // add(decimalType);
            add(asciiType);
            add(textType);
            // TODO: blob is not supported in SAI
            // add(blobType);
            add(uuidType);
            add(timestampType);
            // TODO: SAI test fails due to TimeSerializer#toString in tracing
            //  add(timeType);
            // TODO: compose proper value
            // add(timeUuidType);
        }};
        TYPES = Collections.unmodifiableList(types);
    }

    public static Generator<DataType<?>> regularColumnTypeGen()
    {
        return Generators.pick(TYPES);
    }

    public static Generator<DataType<?>> clusteringColumnTypeGen()
    {
        return Generators.pick(new ArrayList<>(ReversedType.cache.keySet()));
    }

    public static class ReversedType<T> extends DataType<T>
    {
        public static final Map<DataType<?>, ReversedType<?>> cache = new HashMap<>()
        {{
            for (DataType<?> type : TYPES)
                put(type, new ReversedType<>(type));
        }};

        private final DataType<T> baseType;

        public ReversedType(DataType<T> baseType)
        {
            super(baseType.cqlName);
            this.baseType = baseType;
        }

        @Override
        public /* unsigned */ long typeEntropy()
        {
            return baseType.typeEntropy();
        }

        public boolean isReversed()
        {
            return true;
        }

        @Override
        public AbstractType<?> asServerType()
        {
            return org.apache.cassandra.db.marshal.ReversedType.getInstance(baseType.asServerType());
        }

        public static <T> DataType<T> getInstance(DataType<T> type)
        {
            ReversedType<T> t = (ReversedType<T>) cache.get(type);
            if (t == null)
                t = new ReversedType<>(type);
            assert t.baseType == type : String.format("Type mismatch %s != %s", t.baseType, type);
            return t;
        }

        @Override
        public Comparator<T> comparator()
        {
            return baseType.comparator();
        }
    }
}