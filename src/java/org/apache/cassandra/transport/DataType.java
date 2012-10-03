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
package org.apache.cassandra.transport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import com.google.common.base.Charsets;
import org.jboss.netty.buffer.ChannelBuffer;

import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.utils.Pair;

public enum DataType implements OptionCodec.Codecable<DataType>
{
    CUSTOM   (0,  null),
    ASCII    (1,  AsciiType.instance),
    BIGINT   (2,  LongType.instance),
    BLOB     (3,  BytesType.instance),
    BOOLEAN  (4,  BooleanType.instance),
    COUNTER  (5,  CounterColumnType.instance),
    DECIMAL  (6,  DecimalType.instance),
    DOUBLE   (7,  DoubleType.instance),
    FLOAT    (8,  FloatType.instance),
    INT      (9,  Int32Type.instance),
    TEXT     (10, UTF8Type.instance),
    TIMESTAMP(11, DateType.instance),
    UUID     (12, UUIDType.instance),
    VARCHAR  (13, UTF8Type.instance),
    VARINT   (14, IntegerType.instance),
    TIMEUUID (15, TimeUUIDType.instance),
    INET     (16, InetAddressType.instance),
    LIST     (32, null),
    MAP      (33, null),
    SET      (34, null);

    public static final OptionCodec<DataType> codec = new OptionCodec<DataType>(DataType.class);

    private final int id;
    private final AbstractType type;
    private static final Map<AbstractType, DataType> dataTypeMap = new HashMap<AbstractType, DataType>();
    static
    {
        for (DataType type : DataType.values())
        {
            if (type.type != null)
                dataTypeMap.put(type.type, type);
        }
    }

    private DataType(int id, AbstractType type)
    {
        this.id = id;
        this.type = type;
    }

    public int getId()
    {
        return id;
    }

    public Object readValue(ChannelBuffer cb)
    {
        switch (this)
        {
            case CUSTOM:
                return CBUtil.readString(cb);
            case LIST:
                return DataType.toType(codec.decodeOne(cb));
            case SET:
                return DataType.toType(codec.decodeOne(cb));
            case MAP:
                List<AbstractType> l = new ArrayList<AbstractType>(2);
                l.add(DataType.toType(codec.decodeOne(cb)));
                l.add(DataType.toType(codec.decodeOne(cb)));
                return l;
            default:
                return null;
        }
    }

    public void writeValue(Object value, ChannelBuffer cb)
    {
        switch (this)
        {
            case CUSTOM:
                assert value instanceof String;
                cb.writeBytes(CBUtil.stringToCB((String)value));
                break;
            case LIST:
                cb.writeBytes(codec.encodeOne(DataType.fromType((AbstractType)value)));
                break;
            case SET:
                cb.writeBytes(codec.encodeOne(DataType.fromType((AbstractType)value)));
                break;
            case MAP:
                List<AbstractType> l = (List<AbstractType>)value;
                cb.writeBytes(codec.encodeOne(DataType.fromType(l.get(0))));
                cb.writeBytes(codec.encodeOne(DataType.fromType(l.get(1))));
                break;
        }
    }

    public int serializedValueSize(Object value)
    {
        switch (this)
        {
            case CUSTOM:
                return 2 + ((String)value).getBytes(Charsets.UTF_8).length;
            case LIST:
            case SET:
                return codec.oneSerializedSize(DataType.fromType((AbstractType)value));
            case MAP:
                List<AbstractType> l = (List<AbstractType>)value;
                int s = 0;
                s += codec.oneSerializedSize(DataType.fromType(l.get(0)));
                s += codec.oneSerializedSize(DataType.fromType(l.get(1)));
                return s;
            default:
                return 0;
        }
    }

    public static Pair<DataType, Object> fromType(AbstractType type)
    {
        // For CQL3 clients, ReversedType is an implementation detail and they
        // shouldn't have to care about it.
        if (type instanceof ReversedType)
            type = ((ReversedType)type).baseType;

        DataType dt = dataTypeMap.get(type);
        if (dt == null)
        {
            if (type.isCollection())
            {
                if (type instanceof ListType)
                {
                    return Pair.<DataType, Object>create(LIST, ((ListType)type).elements);
                }
                else if (type instanceof MapType)
                {
                    MapType mt = (MapType)type;
                    return Pair.<DataType, Object>create(MAP, Arrays.asList(mt.keys, mt.values));
                }
                else
                {
                    assert type instanceof SetType;
                    return Pair.<DataType, Object>create(SET, ((SetType)type).elements);
                }
            }
            return Pair.<DataType, Object>create(CUSTOM, type.toString());
        }
        else
        {
            return Pair.create(dt, null);
        }
    }

    public static AbstractType toType(Pair<DataType, Object> entry)
    {
        try
        {
            switch (entry.left)
            {
                case CUSTOM:
                    return TypeParser.parse((String)entry.right);
                case LIST:
                    return ListType.getInstance((AbstractType)entry.right);
                case SET:
                    return SetType.getInstance((AbstractType)entry.right);
                case MAP:
                    List<AbstractType> l = (List<AbstractType>)entry.right;
                    return MapType.getInstance(l.get(0), l.get(1));
                default:
                    return entry.left.type;
            }
        }
        catch (RequestValidationException e)
        {
            throw new ProtocolException(e.getMessage());
        }
    }
}
