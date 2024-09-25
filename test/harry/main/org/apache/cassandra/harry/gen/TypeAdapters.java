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

package org.apache.cassandra.harry.gen;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableList;

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
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.schema.ColumnMetadata;

/**
 * A class that helps to translate Cassandra's AbstractType instances to Harry Generators
 */
public class TypeAdapters
{
    public static final Map<AbstractType<?>, Generator<?>> defaults = new HashMap<>() {{
        put(ByteType.instance, Generators.int8());
        put(ShortType.instance, Generators.int16());
        put(Int32Type.instance, Generators.int32());
        put(LongType.instance, Generators.int64());
        put(BooleanType.instance, Generators.bool()); // this type has extremely small entryop
        put(FloatType.instance, Generators.floats());
        put(DoubleType.instance, Generators.doubles());
        put(BytesType.instance, Generators.bytes(10, 20));
        put(AsciiType.instance, Generators.englishAlphabet(5, 10));
        put(UTF8Type.instance, Generators.utf8(10, 20));
        put(UUIDType.instance, Generators.uuidGen());
        put(TimeUUIDType.instance, Generators.timeuuid());
        put(TimestampType.instance, Generators.int64(0, Long.MAX_VALUE).map(Date::new));
        put(TimeType.instance, Generators.int64(0, Long.MAX_VALUE));
        put(IntegerType.instance, Generators.bigInt());
        put(DecimalType.instance, Generators.bigDecimal());
        put(InetAddressType.instance, Generators.inetAddr());

        for (AbstractType<?> type : new ArrayList<>(keySet()))
            put(ReversedType.getInstance(type), get(type));
    }};

    public static Generator<Object[]> forKeys(ImmutableList<ColumnMetadata> columns)
    {
        return forKeys(columns, defaults);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static Generator<Object[]> forKeys(ImmutableList<ColumnMetadata> columns, Map<AbstractType<?>, Generator<?>> typeToGen)
    {
        Generator[] gens = new Generator[columns.size()];
        for (int i = 0; i < gens.length; i++)
        {
            gens[i] = forValues(columns.get(i), typeToGen);
        }
        return Generators.zipArray(gens);
    }

    public static Generator<Object> forValues(ColumnMetadata column)
    {
        return forValues(column, defaults);
    }

    private static Generator<Object> forValues(ColumnMetadata column, Map<AbstractType<?>, Generator<?>> typeToGen)
    {
        Generator<Object> gen = (Generator<Object>) typeToGen.get(column.type);
        if (gen == null)
        {
            throw new IllegalArgumentException(String.format("Could not find generator for column %s of type %s in %s",
                                                             column, column.type, typeToGen));
        }
        return gen;
    }

    public static Generator<Object> forValues(AbstractType type)
    {
        return forValues(type, defaults);
    }

    private static Generator<Object> forValues(AbstractType type, Map<AbstractType<?>, Generator<?>> typeToGen)
    {
        Generator<Object> gen = (Generator<Object>) typeToGen.get(type);
        if (gen == null)
        {
            throw new IllegalArgumentException(String.format("Could not find generator for column type %s in %s",
                                                             type, typeToGen));
        }
        return gen;
    }
}
