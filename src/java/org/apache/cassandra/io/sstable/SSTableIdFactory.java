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

package org.apache.cassandra.io.sstable;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.TimeUUID;

public class SSTableIdFactory
{
    public static final SSTableIdFactory instance = new SSTableIdFactory();

    private static boolean isULIDImpl()
    {
        String impl = CassandraRelevantProperties.SSTABLE_UUID_IMPL.getString().toLowerCase();
        if ("uuid".equals(impl))
            return false;
        else if ("ulid".equals(impl))
            return true;
        else
            throw new IllegalArgumentException("Unsupported value for property " + CassandraRelevantProperties.SSTABLE_UUID_IMPL.getKey() + ": " + impl);
    }

    private Stream<SSTableId.Builder<?>> makeIdBuildersStream()
    {
        return isULIDImpl()
               ? Stream.of(ULIDBasedSSTableId.Builder.instance, UUIDBasedSSTableId.Builder.instance, SequenceBasedSSTableId.Builder.instance)
               : Stream.of(UUIDBasedSSTableId.Builder.instance, ULIDBasedSSTableId.Builder.instance, SequenceBasedSSTableId.Builder.instance);
    }

    /**
     * Constructs the instance of {@link SSTableId} from the given string representation.
     * It finds the right builder by verifying whether the given string is the representation of the related identifier
     * type using {@link SSTableId.Builder#isUniqueIdentifier(String)} method.
     *
     * @throws IllegalArgumentException when the provided string representation does not represent id of any type
     */
    public SSTableId fromString(String str) throws IllegalArgumentException
    {
        return makeIdBuildersStream().filter(b -> b.isUniqueIdentifier(str))
                                     .findFirst()
                                     .map(b -> b.fromString(str))
                                     .orElseThrow(() -> new IllegalArgumentException("String '" + str + "' does not match any SSTable identifier format"));
    }

    /**
     * Constructs the instance of {@link SSTableId} from the given bytes.
     * It finds the right builder by verifying whether the given buffer is the representation of the related identifier
     * type using {@link SSTableId.Builder#isUniqueIdentifier(ByteBuffer)} method.
     * <p>
     * The method expects the identifier is encoded in all remaining bytes of the buffer. The method does not move the
     * pointer of the buffer.
     *
     * @throws IllegalArgumentException when the provided binary representation does not represent id of any type
     */
    public SSTableId fromBytes(ByteBuffer bytes)
    {
        return makeIdBuildersStream().filter(b -> b.isUniqueIdentifier(bytes))
                                     .findFirst()
                                     .map(b -> b.fromBytes(bytes))
                                     .orElseThrow(() -> new IllegalArgumentException("Byte buffer of length " + bytes.remaining() + " does not match any SSTable identifier format"));
    }

    /**
     * Returns default identifiers builder.
     */
    public SSTableId.Builder<? extends SSTableId> defaultBuilder()
    {
        if (DatabaseDescriptor.isUUIDSSTableIdentifiersEnabled())
            return isULIDImpl()
                   ? ULIDBasedSSTableId.Builder.instance
                   : UUIDBasedSSTableId.Builder.instance;
        else
            return SequenceBasedSSTableId.Builder.instance;
    }

    /**
     * Compare sstable identifiers so that UUID based identifier is always greater than sequence based identifier
     */
    public static final Comparator<SSTableId> COMPARATOR = Comparator.nullsFirst(Comparator.comparing(SSTableIdFactory::asTimeUUID));

    private static Pair<TimeUUID, Integer> asTimeUUID(SSTableId id)
    {
        if (id instanceof UUIDBasedSSTableId)
            return Pair.of(((UUIDBasedSSTableId) id).uuid, null);
        else if (id instanceof ULIDBasedSSTableId)
            return Pair.of(((ULIDBasedSSTableId) id).approximateTimeUUID, null);
        else if (id instanceof SequenceBasedSSTableId)
            return Pair.of(null, ((SequenceBasedSSTableId) id).generation);
        else
            throw new AssertionError("Unsupported sstable identifier type " + id.getClass().getName());
    }
}
