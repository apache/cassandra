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

import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.SSTableId;

/**
 * A bidirectional map of {@link PrimaryKey} to row Id. Implementations of this interface
 * are not expected to be threadsafe.
 */
@NotThreadSafe
public interface PrimaryKeyMap extends Closeable
{
    /**
     * A factory for creating {@link PrimaryKeyMap} instances. Implementations of this
     * interface are expected to be threadsafe.
     */
    public interface Factory extends Closeable
    {
        /**
         * Creates a new {@link PrimaryKeyMap} instance
         *
         * @return a {@link PrimaryKeyMap}
         * @throws IOException
         */
        PrimaryKeyMap newPerSSTablePrimaryKeyMap();

        @Override
        default void close() throws IOException
        {
        }
    }

    /**
     * Returns the {@link SSTableId} associated with this {@link PrimaryKeyMap}
     * @return an {@link SSTableId}
     */
    SSTableId<?> getSSTableId();

    /**
     * Returns a {@link PrimaryKey} for a row Id
     *
     * @param sstableRowId the row Id to lookup
     * @return the {@link PrimaryKey} associated with the row Id
     */
    PrimaryKey primaryKeyFromRowId(long sstableRowId);

    /**
     * Returns a row Id for a {@link PrimaryKey}. If there is no such term, returns the `-(next row id) - 1` where
     * `next row id` is the row id of the next greatest {@link PrimaryKey} in the map.
     *
     * @param key the {@link PrimaryKey} to lookup
     * @return the row Id associated with the {@link PrimaryKey}
     */
    long exactRowIdOrInvertedCeiling(PrimaryKey key);

    /**
     * Returns the sstable row id associated with the least {@link PrimaryKey} greater than or equal to the given
     * {@link PrimaryKey}. If the {@link PrimaryKey} is a prefix of multiple {@link PrimaryKey}s in the map, e.g. it is
     * just a token or a token and a partition key, the row id associated with the least {@link PrimaryKey} will be
     * returned. If there is no {@link PrimaryKey} in the map that meets this definition, returns a negative value.
     *
     * @param key the {@link PrimaryKey} to lookup
     * @return an sstable row id or a negative value if no row is found
     */
    long ceiling(PrimaryKey key);

    /**
     * Returns the sstable row id associated with the greatest {@link PrimaryKey} less than or equal to the given
     * {@link PrimaryKey}. If the {@link PrimaryKey} is a prefix of multiple {@link PrimaryKey}s in the map, e.g. it is
     * just a token or a token and a partition key, the row id associated with the greatest {@link PrimaryKey} will be
     * returned. If there is no {@link PrimaryKey} in the map that meets this definition, returns a negative value.
     *
     * @param key the {@link PrimaryKey} to lookup
     * @return an sstable row id or a negative value if no row is found
     */
    long floor(PrimaryKey key);

    /**
     * Returns the number of primary keys in the map
     */
    long count();

    @Override
    default void close() throws IOException
    {
    }
}
