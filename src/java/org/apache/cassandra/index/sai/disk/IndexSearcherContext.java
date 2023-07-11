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

import java.io.IOException;

import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

public class IndexSearcherContext
{
    final QueryContext context;
    final PostingList.PeekablePostingList postingList;

    final PrimaryKey minimumKey;
    final PrimaryKey maximumKey;
    final long minSSTableRowId;
    final long maxSSTableRowId;
    final long segmentRowIdOffset;
    final long maxPartitionOffset;

    public IndexSearcherContext(PrimaryKey minimumKey,
                                PrimaryKey maximumKey,
                                long minSSTableRowId,
                                long maxSSTableRowId,
                                long segmentRowIdOffset,
                                QueryContext context,
                                PostingList.PeekablePostingList postingList) throws IOException
    {
        this.context = context;
        this.postingList = postingList;

        this.segmentRowIdOffset = segmentRowIdOffset;

        this.minimumKey = minimumKey;

        // use segment's metadata for the range iterator, may not be accurate, but should not matter to performance.
        this.maximumKey = maximumKey;

        this.minSSTableRowId = minSSTableRowId;
        this.maxSSTableRowId = maxSSTableRowId;
        this.maxPartitionOffset = Long.MAX_VALUE;
    }

    long count()
    {
        return postingList.size();
    }
}
