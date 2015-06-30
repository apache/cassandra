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
package org.apache.cassandra.db.partitions;

import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.DataLimits;

public class CountingUnfilteredRowIterator extends WrappingUnfilteredRowIterator
{
    private final DataLimits.Counter counter;

    public CountingUnfilteredRowIterator(UnfilteredRowIterator iter, DataLimits.Counter counter)
    {
        super(iter);
        this.counter = counter;

        counter.newPartition(iter.partitionKey(), iter.staticRow());
    }

    public DataLimits.Counter counter()
    {
        return counter;
    }

    @Override
    public boolean hasNext()
    {
        if (counter.isDoneForPartition())
            return false;

        return super.hasNext();
    }

    @Override
    public Unfiltered next()
    {
        Unfiltered next = super.next();
        if (next.isRow())
            counter.newRow((Row)next);
        return next;
    }

    @Override
    public void close()
    {
        super.close();
        counter.endOfPartition();
    }
}
