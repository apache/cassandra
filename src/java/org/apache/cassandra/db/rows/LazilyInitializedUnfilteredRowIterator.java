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
package org.apache.cassandra.db.rows;

import org.apache.cassandra.utils.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;

/**
 * Abstract class to create UnfilteredRowIterator that lazily initialize themselves.
 *
 * This is used during partition range queries when we know the partition key but want
 * to defer the initialization of the rest of the UnfilteredRowIterator until we need those informations.
 * See {@link org.apache.cassandra.io.sstable.format.big.BigTableScanner#KeyScanningIterator} for instance.
 */
public abstract class LazilyInitializedUnfilteredRowIterator extends AbstractIterator<Unfiltered> implements UnfilteredRowIterator
{
    private final DecoratedKey partitionKey;

    private UnfilteredRowIterator iterator;

    public LazilyInitializedUnfilteredRowIterator(DecoratedKey partitionKey)
    {
        this.partitionKey = partitionKey;
    }

    protected abstract UnfilteredRowIterator initializeIterator();

    protected void maybeInit()
    {
        if (iterator == null)
            iterator = initializeIterator();
    }

    public boolean initialized()
    {
        return iterator != null;
    }

    public CFMetaData metadata()
    {
        maybeInit();
        return iterator.metadata();
    }

    public PartitionColumns columns()
    {
        maybeInit();
        return iterator.columns();
    }

    public boolean isReverseOrder()
    {
        maybeInit();
        return iterator.isReverseOrder();
    }

    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    public DeletionTime partitionLevelDeletion()
    {
        maybeInit();
        return iterator.partitionLevelDeletion();
    }

    public Row staticRow()
    {
        maybeInit();
        return iterator.staticRow();
    }

    public EncodingStats stats()
    {
        maybeInit();
        return iterator.stats();
    }

    protected Unfiltered computeNext()
    {
        maybeInit();
        return iterator.hasNext() ? iterator.next() : endOfData();
    }

    public void close()
    {
        if (iterator != null)
            iterator.close();
    }
}
