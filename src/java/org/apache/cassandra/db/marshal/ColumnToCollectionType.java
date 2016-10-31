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
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.BytesSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

/*
 * This class is deprecated and only kept for backward compatibility.
 */
public class ColumnToCollectionType extends AbstractType<ByteBuffer>
{
    // interning instances
    private static final Map<Map<ByteBuffer, CollectionType>, ColumnToCollectionType> instances = new HashMap<>();

    public final Map<ByteBuffer, CollectionType> defined;

    public static ColumnToCollectionType getInstance(TypeParser parser) throws SyntaxException, ConfigurationException
    {
        return getInstance(parser.getCollectionsParameters());
    }

    public static synchronized ColumnToCollectionType getInstance(Map<ByteBuffer, CollectionType> defined)
    {
        assert defined != null;

        ColumnToCollectionType t = instances.get(defined);
        if (t == null)
        {
            t = new ColumnToCollectionType(defined);
            instances.put(defined, t);
        }
        return t;
    }

    private ColumnToCollectionType(Map<ByteBuffer, CollectionType> defined)
    {
        super(ComparisonType.CUSTOM);
        this.defined = ImmutableMap.copyOf(defined);
    }

    public int compareCustom(ByteBuffer o1, ByteBuffer o2)
    {
        throw new UnsupportedOperationException("ColumnToCollectionType should only be used in composite types, never alone");
    }

    public int compareCollectionMembers(ByteBuffer o1, ByteBuffer o2, ByteBuffer collectionName)
    {
        CollectionType t = defined.get(collectionName);
        if (t == null)
            throw new RuntimeException(ByteBufferUtil.bytesToHex(collectionName) + " is not defined as a collection");

        return t.nameComparator().compare(o1, o2);
    }

    public String getString(ByteBuffer bytes)
    {
        return BytesType.instance.getString(bytes);
    }

    public ByteBuffer fromString(String source)
    {
        try
        {
            return ByteBufferUtil.hexToBytes(source);
        }
        catch (NumberFormatException e)
        {
            throw new MarshalException(String.format("cannot parse '%s' as hex bytes", source), e);
        }
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void validate(ByteBuffer bytes)
    {
        throw new UnsupportedOperationException("ColumnToCollectionType should only be used in composite types, never alone");
    }

    public TypeSerializer<ByteBuffer> getSerializer()
    {
        return BytesSerializer.instance;
    }

    public void validateCollectionMember(ByteBuffer bytes, ByteBuffer collectionName) throws MarshalException
    {
        CollectionType t = defined.get(collectionName);
        if (t == null)
            throw new MarshalException(ByteBufferUtil.bytesToHex(collectionName) + " is not defined as a collection");

        t.nameComparator().validate(bytes);
    }

    @Override
    public boolean isCompatibleWith(AbstractType<?> previous)
    {
        if (!(previous instanceof ColumnToCollectionType))
            return false;

        ColumnToCollectionType prev = (ColumnToCollectionType)previous;
        // We are compatible if we have all the definitions previous have (but we can have more).
        for (Map.Entry<ByteBuffer, CollectionType> entry : prev.defined.entrySet())
        {
            CollectionType newType = defined.get(entry.getKey());
            if (newType == null || !newType.isCompatibleWith(entry.getValue()))
                return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return getClass().getName() + TypeParser.stringifyCollectionsParameters(defined);
    }
}
