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

import org.apache.cassandra.config.DatabaseDescriptor;

public class SSTableIdFactory
{
    public static final SSTableIdFactory instance = new SSTableIdFactory();

    /**
     * Constructs the instance of {@link SSTableId} from the given string representation.
     * It finds the right builder by verifying whether the given string is the representation of the related identifier
     * type using {@link SSTableId.Builder#isUniqueIdentifier(String)} method.
     *
     * @throws IllegalArgumentException when the provided string representation does not represent id of any type
     */
    public SSTableId fromString(String str) throws IllegalArgumentException
    {
        return Stream.of(UUIDBasedSSTableId.Builder.instance, SequenceBasedSSTableId.Builder.instance)
                     .filter(b -> b.isUniqueIdentifier(str))
                     .findFirst()
                     .map(b -> b.fromString(str))
                     .orElseThrow(() -> new IllegalArgumentException("String '" + str + "' does not match any SSTable identifier format"));
    }

    /**
     * Constructs the instance of {@link SSTableId} from the given bytes.
     * It finds the right builder by verifying whether the given buffer is the representation of the related identifier
     * type using {@link SSTableId.Builder#isUniqueIdentifier(ByteBuffer)} method.
     *
     * The method expects the identifier is encoded in all remaining bytes of the buffer. The method does not move the
     * pointer of the buffer.
     *
     * @throws IllegalArgumentException when the provided binary representation does not represent id of any type
     */
    public SSTableId fromBytes(ByteBuffer bytes)
    {
        return Stream.of(UUIDBasedSSTableId.Builder.instance, SequenceBasedSSTableId.Builder.instance)
                     .filter(b -> b.isUniqueIdentifier(bytes))
                     .findFirst()
                     .map(b -> b.fromBytes(bytes))
                     .orElseThrow(() -> new IllegalArgumentException("Byte buffer of length " + bytes.remaining() + " does not match any SSTable identifier format"));
    }

    /**
     * Returns default identifiers builder.
     */
    @SuppressWarnings("unchecked")
    public SSTableId.Builder<SSTableId> defaultBuilder()
    {
        SSTableId.Builder<? extends SSTableId> builder = DatabaseDescriptor.isUUIDSSTableIdentifiersEnabled()
                                                         ? UUIDBasedSSTableId.Builder.instance
                                                         : SequenceBasedSSTableId.Builder.instance;
        return (SSTableId.Builder<SSTableId>) builder;
    }

    /**
     * Compare sstable identifiers so that UUID based identifier is always greater than sequence based identifier
     */
    public final static Comparator<SSTableId> COMPARATOR = Comparator.nullsFirst((id1, id2) -> {
        if (id1 instanceof UUIDBasedSSTableId)
        {
            UUIDBasedSSTableId uuidId1 = (UUIDBasedSSTableId) id1;
            return (id2 instanceof UUIDBasedSSTableId) ? uuidId1.compareTo((UUIDBasedSSTableId) id2) : 1;
        }
        else if (id1 instanceof SequenceBasedSSTableId)
        {
            SequenceBasedSSTableId seqId1 = (SequenceBasedSSTableId) id1;
            return (id2 instanceof SequenceBasedSSTableId) ? seqId1.compareTo((SequenceBasedSSTableId) id2) : -1;
        }
        else
        {
            throw new AssertionError("Unsupported comparison between " + id1.getClass().getName() + " and  " + id2.getClass().getName());
        }
    });
}
