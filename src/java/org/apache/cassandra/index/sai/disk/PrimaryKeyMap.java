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

package org.apache.cassandra.index.sai.disk;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

/**
 * A bidirectional map of {@link PrimaryKey} to row ID. Implementations of this interface
 * are not expected to be threadsafe.
 */
@NotThreadSafe
public interface PrimaryKeyMap extends Closeable
{
    /**
     * A factory for creating {@link PrimaryKeyMap} instances. Implementations of this
     * interface are expected to be threadsafe.
     */
    @ThreadSafe
    interface Factory extends Closeable
    {
        /**
         * Creates a new {@link PrimaryKeyMap} instance
         *
         * @return a {@link PrimaryKeyMap}
         * @throws IOException if the {@link PrimaryKeyMap} couldn't be created
         */
        PrimaryKeyMap newPerSSTablePrimaryKeyMap() throws IOException;

        @Override
        default void close()
        {
        }
    }

    /**
     * Returns a {@link PrimaryKey} for a row ID
     *
     * @param sstableRowId the row ID to lookup
     * @return the {@link PrimaryKey} associated with the row ID
     */
    PrimaryKey primaryKeyFromRowId(long sstableRowId);

    /**
     * Returns a row ID for a {@link PrimaryKey}
     *
     * @param key the {@link PrimaryKey} to lookup
     * @return the row ID associated with the {@link PrimaryKey}
     */
    long rowIdFromPrimaryKey(PrimaryKey key);

    /**
     * Returns the first row ID of the nearest {@link Token} greater than or equal to the given {@link Token},
     * or a negative value if not found
     *
     * @param token the {@link Token} to lookup
     * @return the ceiling row ID associated with the {@link Token} or a negative value
     */
    long ceiling(Token token);

    /**
     * Returns the last row ID of the nearest {@link Token} less than or equal to the given {@link Token},
     * or a negative value if the {@link Token} is at its minimum value
     *
     * @param token the {@link Token} to lookup
     * @return the floor row ID associated with the {@link Token}
     */
    long floor(Token token);

    @Override
    default void close()
    {
    }
}
