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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.hnsw.CassandraOnDiskHnsw;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.lucene.util.Bits;

/**
 * Tracks SSTable-specific state relevant to the execution of a single query.
 *
 * Fields here are non-volatile, as they are accessed from a single thread.
 */
@NotThreadSafe
public class SSTableQueryContext
{
    public final QueryContext queryContext;

    public SSTableQueryContext(QueryContext queryContext)
    {
        this.queryContext = queryContext;
    }

    @VisibleForTesting
    public static SSTableQueryContext forTest()
    {
        return new SSTableQueryContext(new QueryContext());
    }

    /**
     * @return true to include current sstable row id; otherwise false if the sstable row id will be shadowed
     */
    public boolean shouldInclude(Long sstableRowId, PrimaryKeyMap primaryKeyMap)
    {
        return !queryContext.containsShadowedPrimaryKey(() -> primaryKeyMap.primaryKeyFromRowId(sstableRowId));
    }

    /**
     * Create a bitset to ignore ordinals corresponding to shadowed primary keys
     */
    public Bits bitsForShadowedPrimaryKeys(SegmentMetadata metadata, PrimaryKeyMap primaryKeyMap, CassandraOnDiskHnsw graph) throws IOException
    {
        return queryContext.bitsetForShadowedPrimaryKeys(metadata, primaryKeyMap, graph);
    }
}
