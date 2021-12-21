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

import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

/**
 * A bidirectional map of {@link PrimaryKey} to row Id. Implementations of this interface
 * are not expected to be threadsafe.
 */
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
         * @param context the context used to record query time metrics and for caching
         * @return a {@link PrimaryKeyMap}
         * @throws IOException
         */
        PrimaryKeyMap newPerSSTablePrimaryKeyMap(SSTableQueryContext context) throws IOException;

        @Override
        default void close() throws IOException
        {
        }
    }

    /**
     * Returns a {@link PrimaryKey} for a row Id
     *
     * @param sstableRowId the row Id to lookup
     * @return the {@link PrimaryKey} associated with the row Id
     */
    PrimaryKey primaryKeyFromRowId(long sstableRowId);

    /**
     * Returns a row Id for a {@link PrimaryKey}
     *
     * @param key the {@link PrimaryKey} to lookup
     * @return the row Id associated with the {@link PrimaryKey}
     */
    long rowIdFromPrimaryKey(PrimaryKey key);

    @Override
    default void close() throws IOException
    {
    }
}
