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
package org.apache.cassandra.db.index;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * Base class for Secondary indexes that implement a unique index per column
 *
 */
public abstract class PerColumnSecondaryIndex extends SecondaryIndex
{
    /**
     * Called when a column has been tombstoned or replaced.
     *
     * @param rowKey the underlying row key which is indexed
     * @param col all the column info
     */
    public abstract void delete(ByteBuffer rowKey, Clustering clustering, Cell cell, OpOrder.Group opGroup, int nowInSec);

    /**
     * Called when a column has been removed due to a cleanup operation.
     */
    public abstract void deleteForCleanup(ByteBuffer rowKey, Clustering clustering, Cell cell, OpOrder.Group opGroup, int nowInSec);

    /**
     * For indexes on the primary key, index the given PK.
     */
    public void maybeIndex(ByteBuffer partitionKey, Clustering clustering, long timestamp, int ttl, OpOrder.Group opGroup, int nowInSec)
    {
    }

    /**
     * For indexes on the primary key, delete the given PK.
     */
    public void maybeDelete(ByteBuffer partitionKey, Clustering clustering, DeletionTime deletion, OpOrder.Group opGroup)
    {
    }

    /**
     * insert a column to the index
     *
     * @param rowKey the underlying row key which is indexed
     * @param col all the column info
     */
    public abstract void insert(ByteBuffer rowKey, Clustering clustering, Cell cell, OpOrder.Group opGroup);

    /**
     * update a column from the index
     *
     * @param rowKey the underlying row key which is indexed
     * @param oldCol the previous column info
     * @param col all the column info
     */
    public abstract void update(ByteBuffer rowKey, Clustering clustering, Cell oldCell, Cell cell, OpOrder.Group opGroup, int nowInSec);

    protected boolean indexPrimaryKeyColumn()
    {
        return false;
    }

    public void indexRow(DecoratedKey key, Row row, OpOrder.Group opGroup, int nowInSec)
    {
        Clustering clustering = row.clustering();
        if (indexPrimaryKeyColumn())
        {
            // Same as in AtomicBTreePartition.maybeIndexPrimaryKeyColumn
            long timestamp = row.primaryKeyLivenessInfo().timestamp();
            int ttl = row.primaryKeyLivenessInfo().ttl();

            for (Cell cell : row)
            {
                if (cell.isLive(nowInSec) && cell.livenessInfo().timestamp() > timestamp)
                {
                    timestamp = cell.livenessInfo().timestamp();
                    ttl = cell.livenessInfo().ttl();
                }
            }
            maybeIndex(key.getKey(), clustering, timestamp, ttl, opGroup, nowInSec);
        }
        for (Cell cell : row)
        {
            if (!indexes(cell.column()))
                continue;

            if (cell.isLive(nowInSec))
                insert(key.getKey(), clustering, cell, opGroup);
        }
    }

    public String getNameForSystemKeyspace(ByteBuffer column)
    {
        return getIndexName();
    }
}
