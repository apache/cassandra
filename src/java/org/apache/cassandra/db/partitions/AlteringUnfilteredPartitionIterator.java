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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.*;

/**
 * A partition iterator that allows to filter/modify the unfiltered from the
 * underlying iterators.
 */
public abstract class AlteringUnfilteredPartitionIterator extends WrappingUnfilteredPartitionIterator
{
    protected AlteringUnfilteredPartitionIterator(UnfilteredPartitionIterator wrapped)
    {
        super(wrapped);
    }

    protected Row computeNextStatic(DecoratedKey partitionKey, Row row)
    {
        return row;
    }

    protected Row computeNext(DecoratedKey partitionKey, Row row)
    {
        return row;
    }

    protected RangeTombstoneMarker computeNext(DecoratedKey partitionKey, RangeTombstoneMarker marker)
    {
        return marker;
    }

    @Override
    protected UnfilteredRowIterator computeNext(UnfilteredRowIterator iter)
    {
        final DecoratedKey partitionKey = iter.partitionKey();
        return new AlteringUnfilteredRowIterator(iter)
        {
            protected Row computeNextStatic(Row row)
            {
                return AlteringUnfilteredPartitionIterator.this.computeNextStatic(partitionKey, row);
            }

            protected Row computeNext(Row row)
            {
                return AlteringUnfilteredPartitionIterator.this.computeNext(partitionKey, row);
            }

            protected RangeTombstoneMarker computeNext(RangeTombstoneMarker marker)
            {
                return AlteringUnfilteredPartitionIterator.this.computeNext(partitionKey, marker);
            }
        };
    }
}

