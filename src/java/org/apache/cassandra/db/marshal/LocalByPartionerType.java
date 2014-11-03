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

import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.MarshalException;

import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.ByteBufferUtil;

/** for sorting columns representing row keys in the row ordering as determined by a partitioner.
 * Not intended for user-defined CFs, and will in fact error out if used with such. */
public class LocalByPartionerType extends AbstractType<ByteBuffer>
{
    private final IPartitioner partitioner;

    public LocalByPartionerType(IPartitioner partitioner)
    {
        this.partitioner = partitioner;
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

    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        // o1 and o2 can be empty so we need to use RowPosition, not DecoratedKey
        return RowPosition.ForKey.get(o1, partitioner).compareTo(RowPosition.ForKey.get(o2, partitioner));
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
}
