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

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Charsets;
import org.jboss.netty.buffer.ChannelBuffer;

import org.apache.cassandra.config.ConfigurationException;
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
    TIMEUUID (15, TimeUUIDType.instance);

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
        }
    }

    public int serializedValueSize(Object value)
    {
        switch (this)
        {
            case CUSTOM:
                return 2 + ((String)value).getBytes(Charsets.UTF_8).length;
            default:
                return 0;
        }
    }

    public static Pair<DataType, Object> fromType(AbstractType type)
    {
        DataType dt = dataTypeMap.get(type);
        if (dt == null)
            return Pair.<DataType, Object>create(CUSTOM, type.toString());
        else
            return Pair.create(dt, null);
    }

    public static AbstractType toType(Pair<DataType, Object> entry)
    {
        try
        {
            if (entry.left == CUSTOM)
                return TypeParser.parse((String)entry.right);
            else
                return entry.left.type;
        }
        catch (ConfigurationException e)
        {
            throw new ProtocolException(e.getMessage());
        }
    }
}
