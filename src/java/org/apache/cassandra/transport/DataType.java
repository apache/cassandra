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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;

import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.utils.Pair;

public enum DataType implements OptionCodec.Codecable<DataType>
{
    CUSTOM   (0,  null, 1),
    ASCII    (1,  AsciiType.instance, 1),
    BIGINT   (2,  LongType.instance, 1),
    BLOB     (3,  BytesType.instance, 1),
    BOOLEAN  (4,  BooleanType.instance, 1),
    COUNTER  (5,  CounterColumnType.instance, 1),
    DECIMAL  (6,  DecimalType.instance, 1),
    DOUBLE   (7,  DoubleType.instance, 1),
    FLOAT    (8,  FloatType.instance, 1),
    INT      (9,  Int32Type.instance, 1),
    TEXT     (10, UTF8Type.instance, 1),
    TIMESTAMP(11, TimestampType.instance, 1),
    UUID     (12, UUIDType.instance, 1),
    VARCHAR  (13, UTF8Type.instance, 1),
    VARINT   (14, IntegerType.instance, 1),
    TIMEUUID (15, TimeUUIDType.instance, 1),
    INET     (16, InetAddressType.instance, 1),
    DATE     (17, SimpleDateType.instance, 4),
    TIME     (18, TimeType.instance, 4),
    SMALLINT (19, ShortType.instance, 4),
    BYTE     (20, ByteType.instance, 4),
    LIST     (32, null, 1),
    MAP      (33, null, 1),
    SET      (34, null, 1),
    UDT      (48, null, 3),
    TUPLE    (49, null, 3);

    public static final OptionCodec<DataType> codec = new OptionCodec<DataType>(DataType.class);

    private final int id;
    private final int protocolVersion;
    private final AbstractType type;
    private final Pair<DataType, Object> pair;
    private static final Map<AbstractType, DataType> dataTypeMap = new HashMap<AbstractType, DataType>();
    static
    {
        for (DataType type : DataType.values())
        {
            if (type.type != null)
                dataTypeMap.put(type.type, type);
        }
    }

    DataType(int id, AbstractType type, int protocolVersion)
    {
        this.id = id;
        this.type = type;
        this.protocolVersion = protocolVersion;
        pair = Pair.create(this, null);
    }

    public int getId(int version)
    {
        if (version < protocolVersion)
            return DataType.CUSTOM.getId(version);
        return id;
    }

    public Object readValue(ByteBuf cb, int version)
    {
        switch (this)
        {
            case CUSTOM:
                return CBUtil.readString(cb);
            case LIST:
                return DataType.toType(codec.decodeOne(cb, version));
            case SET:
                return DataType.toType(codec.decodeOne(cb, version));
            case MAP:
                List<AbstractType> l = new ArrayList<AbstractType>(2);
                l.add(DataType.toType(codec.decodeOne(cb, version)));
                l.add(DataType.toType(codec.decodeOne(cb, version)));
                return l;
            case UDT:
                String ks = CBUtil.readString(cb);
                ByteBuffer name = UTF8Type.instance.decompose(CBUtil.readString(cb));
                int n = cb.readUnsignedShort();
                List<ByteBuffer> fieldNames = new ArrayList<>(n);
                List<AbstractType<?>> fieldTypes = new ArrayList<>(n);
                for (int i = 0; i < n; i++)
                {
                    fieldNames.add(UTF8Type.instance.decompose(CBUtil.readString(cb)));
                    fieldTypes.add(DataType.toType(codec.decodeOne(cb, version)));
                }
                return new UserType(ks, name, fieldNames, fieldTypes, true);
            case TUPLE:
                n = cb.readUnsignedShort();
                List<AbstractType<?>> types = new ArrayList<>(n);
                for (int i = 0; i < n; i++)
                    types.add(DataType.toType(codec.decodeOne(cb, version)));
                return new TupleType(types);
            default:
                return null;
        }
    }

    public void writeValue(Object value, ByteBuf cb, int version)
    {
        // Serialize as CUSTOM if client on the other side's version is < required for type
        if (version < protocolVersion)
        {
            CBUtil.writeString(value.toString(), cb);
            return;
        }

        switch (this)
        {
            case CUSTOM:
                assert value instanceof String;
                CBUtil.writeString((String)value, cb);
                break;
            case LIST:
                codec.writeOne(DataType.fromType((AbstractType)value, version), cb, version);
                break;
            case SET:
                codec.writeOne(DataType.fromType((AbstractType)value, version), cb, version);
                break;
            case MAP:
                List<AbstractType> l = (List<AbstractType>)value;
                codec.writeOne(DataType.fromType(l.get(0), version), cb, version);
                codec.writeOne(DataType.fromType(l.get(1), version), cb, version);
                break;
            case UDT:
                UserType udt = (UserType)value;
                CBUtil.writeString(udt.keyspace, cb);
                CBUtil.writeString(UTF8Type.instance.compose(udt.name), cb);
                cb.writeShort(udt.size());
                for (int i = 0; i < udt.size(); i++)
                {
                    CBUtil.writeString(UTF8Type.instance.compose(udt.fieldName(i)), cb);
                    codec.writeOne(DataType.fromType(udt.fieldType(i), version), cb, version);
                }
                break;
            case TUPLE:
                TupleType tt = (TupleType)value;
                cb.writeShort(tt.size());
                for (int i = 0; i < tt.size(); i++)
                    codec.writeOne(DataType.fromType(tt.type(i), version), cb, version);
                break;
        }
    }

    public int serializedValueSize(Object value, int version)
    {
        // Serialize as CUSTOM if client on the other side's version is < required for type
        if (version < protocolVersion)
            return CBUtil.sizeOfString(value.toString());

        switch (this)
        {
            case CUSTOM:
                return CBUtil.sizeOfString((String)value);
            case LIST:
            case SET:
                return codec.oneSerializedSize(DataType.fromType((AbstractType)value, version), version);
            case MAP:
                List<AbstractType> l = (List<AbstractType>)value;
                int s = 0;
                s += codec.oneSerializedSize(DataType.fromType(l.get(0), version), version);
                s += codec.oneSerializedSize(DataType.fromType(l.get(1), version), version);
                return s;
            case UDT:
                UserType udt = (UserType)value;
                int size = 0;
                size += CBUtil.sizeOfString(udt.keyspace);
                size += CBUtil.sizeOfString(UTF8Type.instance.compose(udt.name));
                size += 2;
                for (int i = 0; i < udt.size(); i++)
                {
                    size += CBUtil.sizeOfString(UTF8Type.instance.compose(udt.fieldName(i)));
                    size += codec.oneSerializedSize(DataType.fromType(udt.fieldType(i), version), version);
                }
                return size;
            case TUPLE:
                TupleType tt = (TupleType)value;
                size = 2;
                for (int i = 0; i < tt.size(); i++)
                    size += codec.oneSerializedSize(DataType.fromType(tt.type(i), version), version);
                return size;
            default:
                return 0;
        }
    }

    public static Pair<DataType, Object> fromType(AbstractType type, int version)
    {
        // For CQL3 clients, ReversedType is an implementation detail and they
        // shouldn't have to care about it.
        if (type instanceof ReversedType)
            type = ((ReversedType)type).baseType;

        // For compatibility sake, we still return DateType as the timestamp type in resultSet metadata (#5723)
        if (type instanceof DateType)
            type = TimestampType.instance;

        DataType dt = dataTypeMap.get(type);
        if (dt == null)
        {
            if (type.isCollection())
            {
                if (type instanceof ListType)
                {
                    return Pair.<DataType, Object>create(LIST, ((ListType)type).getElementsType());
                }
                else if (type instanceof MapType)
                {
                    MapType mt = (MapType)type;
                    return Pair.<DataType, Object>create(MAP, Arrays.asList(mt.getKeysType(), mt.getValuesType()));
                }
                else if (type instanceof SetType)
                {
                    return Pair.<DataType, Object>create(SET, ((SetType)type).getElementsType());
                }
                throw new AssertionError();
            }

            if (type instanceof UserType && version >= UDT.protocolVersion)
                return Pair.<DataType, Object>create(UDT, type);

            if (type instanceof TupleType && version >= TUPLE.protocolVersion)
                return Pair.<DataType, Object>create(TUPLE, type);

            return Pair.<DataType, Object>create(CUSTOM, type.toString());
        }
        else
        {
            // Fall back to CUSTOM if target doesn't know this data type
            if (version < dt.protocolVersion)
                return Pair.<DataType, Object>create(CUSTOM, type.toString());
            return dt.pair;
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
                    return ListType.getInstance((AbstractType)entry.right, true);
                case SET:
                    return SetType.getInstance((AbstractType)entry.right, true);
                case MAP:
                    List<AbstractType> l = (List<AbstractType>)entry.right;
                    return MapType.getInstance(l.get(0), l.get(1), true);
                case UDT:
                    return (AbstractType)entry.right;
                case TUPLE:
                    return (AbstractType)entry.right;
                default:
                    return entry.left.type;
            }
        }
        catch (RequestValidationException e)
        {
            throw new ProtocolException(e.getMessage());
        }
    }

    @VisibleForTesting
    public int getProtocolVersion()
    {
        return protocolVersion;
    }
}
