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

package org.apache.cassandra.index.sai.disk.v1.segment;

import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.postings.PeekablePostingList;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

public class IndexSegmentSearcherContext
{
    public final QueryContext context;
    public final PeekablePostingList postingList;

    public final PrimaryKey minimumKey;
    public final PrimaryKey maximumKey;
    public final long segmentRowIdOffset;

    public IndexSegmentSearcherContext(PrimaryKey minimumKey,
                                       PrimaryKey maximumKey,
                                       long segmentRowIdOffset,
                                       QueryContext context,
                                       PeekablePostingList postingList)
    {
        this.context = context;
        this.postingList = postingList;

        this.segmentRowIdOffset = segmentRowIdOffset;

        this.minimumKey = minimumKey;

        // use segment's metadata for the range iterator, may not be accurate, but should not matter to performance.
        this.maximumKey = maximumKey;
    }

    public long count()
    {
        return postingList.size();
    }
}
