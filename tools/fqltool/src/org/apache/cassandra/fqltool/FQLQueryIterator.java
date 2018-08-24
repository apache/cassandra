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

package org.apache.cassandra.fqltool;

import java.util.PriorityQueue;

import net.openhft.chronicle.queue.ExcerptTailer;
import org.apache.cassandra.utils.AbstractIterator;

public class FQLQueryIterator extends AbstractIterator<FQLQuery>
{
    // use a priority queue to be able to sort the head of the query logs in memory
    private final PriorityQueue<FQLQuery> pq;
    private final ExcerptTailer tailer;
    private final FQLQueryReader reader;

    /**
     * Create an iterator over the FQLQueries in tailer
     *
     * Reads up to readAhead queries in to memory to be able to sort them (the files are mostly sorted already)
     */
    public FQLQueryIterator(ExcerptTailer tailer, int readAhead)
    {
        assert readAhead > 0 : "readAhead needs to be > 0";
        reader = new FQLQueryReader();
        this.tailer = tailer;
        pq = new PriorityQueue<>(readAhead);
        for (int i = 0; i < readAhead; i++)
        {
            FQLQuery next = readNext();
            if (next != null)
                pq.add(next);
            else
                break;
        }
    }

    protected FQLQuery computeNext()
    {
        FQLQuery q = pq.poll();
        if (q == null)
            return endOfData();
        FQLQuery next = readNext();
        if (next != null)
            pq.add(next);
        return q;
    }

    private FQLQuery readNext()
    {
        if (tailer.readDocument(reader))
            return reader.getQuery();
        return null;
    }
}

