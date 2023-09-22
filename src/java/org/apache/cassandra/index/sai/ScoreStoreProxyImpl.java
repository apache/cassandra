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

import com.carrotsearch.hppc.LongFloatHashMap;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

class ScoreStoreProxyImpl implements ScoreStoreProxy
{
    private final QueryContext queryContext;
    private final LongFloatHashMap scoreMap;

    ScoreStoreProxyImpl(QueryContext queryContext, LongFloatHashMap scoreMap) {
        assert scoreMap != null;
        this.queryContext = queryContext;
        this.scoreMap = scoreMap;
    }

    @Override
    public void mapStoredScoreForRowIdToPrimaryKey(long sstableRowId, PrimaryKey pk) {
        float score = scoreMap.getOrDefault(sstableRowId, -1);
        // The score should always be present in the cache because we just put it there in the previous iterator.
        // A better solution would be to pass the score through the iterator's as metadata associated with a row.
        // However, we cannot do that yet, so we use a map to store and retrieve the score.
        assert score >= 0;
        queryContext.recordScore(pk, score);
    }
}
