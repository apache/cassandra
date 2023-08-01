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

package org.apache.cassandra.index.sai.disk.v1.postings;

import java.io.IOException;

import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.lucene.store.IndexInput;

/**
 * A subclass of the {@link PostingsReader} that does not allow the {@link PostingList} to be
 * advanced and does not support mapping row ids to primary keys.
 *
 * It is used during index merges to sequentially scan the postings in order using {@link #nextPosting}.
 */
public class ScanningPostingsReader extends PostingsReader
{
    public ScanningPostingsReader(IndexInput input, BlocksSummary summary) throws IOException
    {
        super(input, summary, QueryEventListener.PostingListEventListener.NO_OP);
    }

    @Override
    public long advance(long targetRowId)
    {
        throw new UnsupportedOperationException("Cannot advance a scanning postings reader");
    }
}
