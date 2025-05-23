/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index.internal;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Entries in indexes on non-compact tables (tables with composite comparators)
 * can be encapsulated as IndexedEntry instances. These are not used when dealing
 * with indexes on static/compact tables (i.e. KEYS indexes).
 */
public final class IndexEntry implements Index.IndexMatch
{
    public final DecoratedKey indexValue;
    public final Clustering<?> indexClustering;
    public final long timestamp;

    public final DecoratedKey indexedKey;
    public final Clustering<?> indexedEntryClustering;

    public IndexEntry(DecoratedKey indexValue,
                      Clustering<?> indexClustering,
                      long timestamp,
                      DecoratedKey indexedKey,
                      Clustering<?> indexedEntryClustering)
    {
        this.indexValue = indexValue;
        this.indexClustering = indexClustering;
        this.timestamp = timestamp;
        this.indexedKey = indexedKey;
        this.indexedEntryClustering = indexedEntryClustering;
    }

    @Override
    public DecoratedKey key()
    {
        return indexedKey;
    }

    public static int compare(TableMetadata indexMetadata, TableMetadata baseMetadata, IndexEntry left, IndexEntry right)
    {
        int cmp = left.indexValue.compareTo(right.indexValue);
        if (cmp != 0)
            return cmp;

        cmp = indexMetadata.comparator.compare(left.indexClustering, right.indexClustering);
        if (cmp != 0)
            return cmp;

        cmp = left.indexedKey.compareTo(right.indexedKey);
        if (cmp != 0)
            return cmp;

        // Take STATIC rows into account...
        if (!left.indexedEntryClustering.isEmpty() || !right.indexedEntryClustering.isEmpty())
        {
            if (left.indexedEntryClustering.isEmpty())
                return -1;

            if (right.indexedEntryClustering.isEmpty())
                return 1;

            cmp = baseMetadata.comparator.compare(left.indexedEntryClustering, right.indexedEntryClustering);
            if (cmp != 0)
                return cmp;
        }

        return Long.compare(left.timestamp, right.timestamp);
    }
}
