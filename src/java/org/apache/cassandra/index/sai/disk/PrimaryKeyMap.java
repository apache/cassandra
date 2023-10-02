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

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
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
     * Returns the first row ID for a given partition range
     *
     * @param range the {@link AbstractBounds} to lookup
     * @return the first row ID associated with the {@link AbstractBounds}
     */
    default long firstRowIdForRange(AbstractBounds<PartitionPosition> range)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the last row ID for a given partition range or the previous row ID if there was no match for the partition range
     *
     * @param range the {@link AbstractBounds} to lookup
     * @return the last row ID associated with the {@link AbstractBounds}
     */
    default long lastRowIdForRange(AbstractBounds<PartitionPosition> range)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    default void close()
    {
    }
}
