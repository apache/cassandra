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

package org.apache.cassandra.index.sai;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.carrotsearch.hppc.LongFloatHashMap;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

@NotThreadSafe
public interface ScoreStoreProxy
{

    ScoreStoreProxy EMPTY = new ScoreStoreProxy() {};

    /**
     * @param queryContext the query's context, which is used to record the store by primary key
     * @param scoreMap the map from row id to score. Null if the score map is not available, e.g. the query does not
     *                 track scores.
     * @return a {@link ScoreStoreProxy} instance
     */
    static ScoreStoreProxy create(QueryContext queryContext, @Nullable LongFloatHashMap scoreMap)
    {
        return scoreMap == null ? EMPTY : new ScoreStoreProxyImpl(queryContext, scoreMap);
    }

    /**
     * Maps a row's score stored by sstableRowId in the cache to its {@link PrimaryKey} in the {@link QueryContext}
     * score cache.
     * @param sstableRowId - the row's sstable row id
     * @param pk - the row's primary key
     */
    default void mapStoredScoreForRowIdToPrimaryKey(long sstableRowId, PrimaryKey pk) {}
}
