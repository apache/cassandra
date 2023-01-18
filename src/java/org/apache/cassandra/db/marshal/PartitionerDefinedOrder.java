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

import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.ByteBufferUtil;

/** for sorting columns representing row keys in the row ordering as determined by a partitioner.
 * Not intended for user-defined CFs, and will in fact error out if used with such. */
public class PartitionerDefinedOrder extends AbstractType<ByteBuffer>
{
    private final IPartitioner partitioner;
    private final AbstractType<?> baseType;
    
    public PartitionerDefinedOrder(IPartitioner partitioner)
    {
        super(ComparisonType.CUSTOM);
        this.partitioner = partitioner;
        this.baseType = null;
    }

    public PartitionerDefinedOrder(IPartitioner partitioner, AbstractType<?> baseType)
    {
        super(ComparisonType.CUSTOM);
        this.partitioner = partitioner;
        this.baseType = baseType;
    }

    public static AbstractType<?> getInstance(TypeParser parser)
    {
        return parser.getPartitionerDefinedOrder();
    }
    
    public AbstractType<?> withBaseType(AbstractType<?> baseType)
    {
        return new PartitionerDefinedOrder(partitioner, baseType);
    }

    @Override
    public ByteBuffer compose(ByteBuffer bytes)
    {
        throw new UnsupportedOperationException("You can't do this with a local partitioner.");
    }

    @Override
    public ByteBuffer decompose(ByteBuffer bytes)
    {
        throw new UnsupportedOperationException("You can't do this with a local partitioner.");
    }

    public String getString(ByteBuffer bytes)
    {
        return ByteBufferUtil.bytesToHex(bytes);
    }

    public ByteBuffer fromString(String source)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Term fromJSONObject(Object parsed)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toJSONString(ByteBuffer buffer, int protocolVersion)
    {
        assert baseType != null && !baseType.equals(this) : "PartitionerDefinedOrder's toJSONString method need a baseType but now is null or with a not euqal type.";
        return baseType.toJSONString(buffer, protocolVersion);   
    }

    public int compareCustom(ByteBuffer o1, ByteBuffer o2)
    {
        // o1 and o2 can be empty so we need to use PartitionPosition, not DecoratedKey
        return PartitionPosition.ForKey.get(o1, partitioner).compareTo(PartitionPosition.ForKey.get(o2, partitioner));
    }

    @Override
    public void validate(ByteBuffer bytes) throws MarshalException
    {
        throw new IllegalStateException("You shouldn't be validating this.");
    }

    public TypeSerializer<ByteBuffer> getSerializer()
    {
        throw new UnsupportedOperationException("You can't do this with a local partitioner.");
    }

    @Override
    public String toString()
    {
        if(baseType != null)
        {
            return String.format("%s(%s:%s)", getClass().getName(), partitioner.getClass().getName(), baseType.toString());
        }
        return String.format("%s(%s)", getClass().getName(), partitioner.getClass().getName());
    }
    
    public AbstractType<?>  getBaseType()
    {
        return baseType; 
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj instanceof PartitionerDefinedOrder)
        {
            PartitionerDefinedOrder other = (PartitionerDefinedOrder) obj;
            if (baseType == null && other.baseType == null)
            {
                return this.partitioner.equals(other.partitioner);
            }
            else if (baseType != null && other.baseType != null) 
            {
                return this.baseType.equals(other.baseType) && this.partitioner.equals(other.partitioner);
            }
            return false;
        }
        return false;
    }
}
