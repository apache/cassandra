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
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.cassandra.io.util.File;

/**
 * Represents a unique identifier in the sstable descriptor filename.
 * This ensures each sstable file uniqueness for the certain table on a single node. However, new implementations
 * should ensure the uniqueness across the entire cluster. The legacy implementation which does not satisfy cluster-wide
 * uniqueness will be deprecated and eventually removed.
 * <p>
 * A new implementation must adhere to the following invariants:
 * - Must be locally sortable - that is, the comparison must reflect the comparison of generation times of identifiers
 * generated on the same node
 * - String representation must *not* include the {@link Descriptor#FILENAME_SEPARATOR} character, see {@link Descriptor#fromFileWithComponent(File)}
 * - must be case-insensitive because the sstables can be stored on case-insensitive file system
 * <p>
 */
public interface SSTableId
{
    /**
     * Creates a byte format of the identifier that can be parsed by
     * {@link Builder#fromBytes(ByteBuffer)}
     */
    ByteBuffer asBytes();

    /**
     * Creates a String format of the identifier that can be parsed by
     * {@link Builder#fromString(String)}
     * <p>
     * Must not contain any {@link Descriptor#FILENAME_SEPARATOR} character as it is used in the Descriptor
     * see {@link Descriptor#fromFileWithComponent(File)}
     */
    @Override
    String toString();

    /**
     * Builder that can create instances of certain implementation of {@link SSTableId}.
     */
    interface Builder<T extends SSTableId>
    {
        /**
         * Creates a new generator of identifiers. Each supplied value must be different to all the previously generated
         * values and different to all the provided existing identifiers.
         */
        Supplier<T> generator(Stream<SSTableId> existingIdentifiers);

        boolean isUniqueIdentifier(String str);

        boolean isUniqueIdentifier(ByteBuffer bytes);

        /**
         * Creates an identifier instance from its string representation
         *
         * @param str string representation as returned by {@link SSTableId#toString()}
         * @throws IllegalArgumentException when the provided string is not a valid string representation of the identifier
         */
        T fromString(String str) throws IllegalArgumentException;

        /**
         * Creates an identifier instance from its binary representation
         * <p>
         * The method expects the identifier is encoded in all remaining bytes of the buffer. The method does not move the
         * pointer of the buffer.
         *
         * @param bytes binary representation as returned by {@link #asBytes()}
         * @throws IllegalArgumentException when the provided bytes are not a valid binary representation of the identifier
         */
        T fromBytes(ByteBuffer bytes) throws IllegalArgumentException;
    }
}
