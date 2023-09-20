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
import java.util.Objects;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteComparable.Version;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import javax.annotation.Nullable;

/** for sorting columns representing row keys in the row ordering as determined by a partitioner.
 * Not intended for user-defined CFs, and will in fact error out if used with such. */
public class PartitionerDefinedOrder extends AbstractType<ByteBuffer>
{
    private final IPartitioner partitioner;
    private final AbstractType<?> partitionKeyType;
    
    public PartitionerDefinedOrder(IPartitioner partitioner)
    {
        super(ComparisonType.CUSTOM);
        this.partitioner = partitioner;
        this.partitionKeyType = null;
    }

    public PartitionerDefinedOrder(IPartitioner partitioner, AbstractType<?> partitionKeyType)
    {
        super(ComparisonType.CUSTOM);
        this.partitioner = partitioner;
        this.partitionKeyType = partitionKeyType;
    }

    public static AbstractType<?> getInstance(TypeParser parser)
    {
        return parser.getPartitionerDefinedOrder();
    }

    public AbstractType<?> withPartitionKeyType(AbstractType<?> partitionKeyType)
    {
        return new PartitionerDefinedOrder(partitioner, partitionKeyType);
    }
    
    @Override
    public <V> ByteBuffer compose(V value, ValueAccessor<V> accessor)
    {
        throw new UnsupportedOperationException("You can't do this with a local partitioner.");
    }

    @Override
    public ByteBuffer decompose(ByteBuffer value)
    {
        throw new UnsupportedOperationException("You can't do this with a local partitioner.");
    }

    public <V> String getString(V value, ValueAccessor<V> accessor)
    {
        return accessor.toHex(value);
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
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        assert partitionKeyType != null : "PartitionerDefinedOrder's toJSONString method needs a partition key type but now is null.";
        return partitionKeyType.toJSONString(buffer, protocolVersion);
    }

    public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        // o1 and o2 can be empty so we need to use PartitionPosition, not DecoratedKey
        return PartitionPosition.ForKey.get(accessorL.toBuffer(left), partitioner).compareTo(PartitionPosition.ForKey.get(accessorR.toBuffer(right), partitioner));
    }

    @Override
    public <V> ByteSource asComparableBytes(ValueAccessor<V> accessor, V data, Version version)
    {
        // Partitioners work with ByteBuffers only.
        ByteBuffer buf = ByteBufferAccessor.instance.convert(data, accessor);
        if (version != Version.LEGACY)
        {
            // For ByteComparable.Version.OSS50 and above we encode an empty key with a null byte source. This
            // way we avoid the need to special-handle a sentinel value when we decode the byte source for such a key
            // (e.g. for ByteComparable.Version.Legacy we use the minimum key bound of the partitioner's minimum token as
            // a sentinel value, and that results in the need to go twice through the byte source that is being
            // decoded).
            return buf.hasRemaining() ? partitioner.decorateKey(buf).asComparableBytes(version) : null;
        }
        return PartitionPosition.ForKey.get(buf, partitioner).asComparableBytes(version);
    }

    @Override
    public <V> V fromComparableBytes(ValueAccessor<V> accessor, ByteSource.Peekable comparableBytes, ByteComparable.Version version)
    {
        assert version != Version.LEGACY;
        if (comparableBytes == null)
            return accessor.empty();
        byte[] keyBytes = DecoratedKey.keyFromByteSource(comparableBytes, version, partitioner);
        return accessor.valueOf(keyBytes);
    }

    @Override
    public void validate(ByteBuffer bytes) throws MarshalException
    {
        throw new IllegalStateException("You shouldn't be validating this.");
    }

    @Override
    public <V> boolean isNull(V buffer, ValueAccessor<V> accessor)
    {
        return buffer == null || accessor.isEmpty(buffer);
    }

    @Override
    public TypeSerializer<ByteBuffer> getSerializer()
    {
        throw new UnsupportedOperationException("You can't do this with a local partitioner.");
    }

    @Override
    public ArgumentDeserializer getArgumentDeserializer()
    {
        throw new UnsupportedOperationException("You can't do this with a local partitioner.");
    }

    @Override
    public String toString()
    {
        if (partitionKeyType != null && !DatabaseDescriptor.getStorageCompatibilityMode().isBefore(5))
        {
            return String.format("%s(%s:%s)", getClass().getName(), partitioner.getClass().getName(), partitionKeyType);
        }
        // if Cassandra's major version is before 5, use the old behaviour
        return String.format("%s(%s)", getClass().getName(), partitioner.getClass().getName());
    }
    
    @Nullable
    public AbstractType<?>  getPartitionKeyType()
    {
        return partitionKeyType;
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
            return partitioner.equals(other.partitioner) && Objects.equals(partitionKeyType, other.partitionKeyType);
        }
        return false;
    }
}
