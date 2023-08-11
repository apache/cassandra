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
package org.apache.cassandra.index.internal.composites;

import java.nio.ByteBuffer;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.IndexEntry;
import org.apache.cassandra.schema.IndexMetadata;

/**
 * Index the value of a collection cell.
 *
 * This is a lot like an index on REGULAR, except that we also need to make
 * the collection key part of the index entry so that:
 *   1) we don't have to scan the whole collection at query time to know the
 *   entry is stale and if it still satisfies the query.
 *   2) if a collection has multiple time the same value, we need one entry
 *   for each so that if we delete one of the value only we only delete the
 *   entry corresponding to that value.
 */
public class CollectionValueIndex extends CassandraIndex
{
    public CollectionValueIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef)
    {
        super(baseCfs, indexDef);
    }

    public ByteBuffer getIndexedValue(ByteBuffer partitionKey,
                                      Clustering<?> clustering,
                                      CellPath path, ByteBuffer cellValue)
    {
        return cellValue;
    }

    public <T> CBuilder buildIndexClusteringPrefix(ByteBuffer partitionKey,
                                                   ClusteringPrefix<T> prefix,
                                                   CellPath path)
    {
        CBuilder builder = CBuilder.create(getIndexComparator());
        builder.add(partitionKey);
        for (int i = 0; i < prefix.size(); i++)
            builder.add(prefix.get(i), prefix.accessor());

        // When indexing a static column, prefix will be empty but only the
        // partition key is needed at query time.
        // In the non-static case, cell will be present during indexing but
        // not when searching (CASSANDRA-7525).
        if (prefix.size() == baseCfs.metadata().clusteringColumns().size() && path != null)
            builder.add(path.get(0));

        return builder;
    }

    public IndexEntry decodeEntry(DecoratedKey indexedValue, Row indexEntry)
    {
        Clustering<?> clustering = indexEntry.clustering();
        Clustering<?> indexedEntryClustering = null;
        if (getIndexedColumn().isStatic())
            indexedEntryClustering = Clustering.STATIC_CLUSTERING;
        else
        {
            CBuilder builder = CBuilder.create(baseCfs.getComparator());
            for (int i = 0; i < baseCfs.getComparator().size(); i++)
                builder.add(clustering, i + 1);
            indexedEntryClustering = builder.build();
        }

        return new IndexEntry(indexedValue,
                                clustering,
                                indexEntry.primaryKeyLivenessInfo().timestamp(),
                                clustering.bufferAt(0),
                                indexedEntryClustering);
    }

    public boolean supportsOperator(ColumnMetadata indexedColumn, Operator operator)
    {
        return operator == Operator.CONTAINS && !(indexedColumn.type instanceof SetType);
    }

    public boolean isStale(Row data, ByteBuffer indexValue, long nowInSec)
    {
        ColumnMetadata columnDef = indexedColumn;
        ComplexColumnData complexData = data.getComplexColumnData(columnDef);
        if (complexData == null)
            return true;

        for (Cell<?> cell : complexData)
        {
            if (cell.isLive(nowInSec) && ((CollectionType) columnDef.type).valueComparator()
                                                                          .compare(indexValue, cell.buffer()) == 0)
                return false;
        }
        return true;
    }
}
