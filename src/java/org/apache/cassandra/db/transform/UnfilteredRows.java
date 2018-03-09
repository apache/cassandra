/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

final class UnfilteredRows extends BaseRows<Unfiltered, UnfilteredRowIterator> implements UnfilteredRowIterator
{
    private RegularAndStaticColumns regularAndStaticColumns;
    private DeletionTime partitionLevelDeletion;

    public UnfilteredRows(UnfilteredRowIterator input)
    {
        this(input, input.columns());
    }

    public UnfilteredRows(UnfilteredRowIterator input, RegularAndStaticColumns columns)
    {
        super(input);
        regularAndStaticColumns = columns;
        partitionLevelDeletion = input.partitionLevelDeletion();
    }

    @Override
    void add(Transformation add)
    {
        super.add(add);
        regularAndStaticColumns = add.applyToPartitionColumns(regularAndStaticColumns);
        partitionLevelDeletion = add.applyToDeletion(partitionLevelDeletion);
    }

    @Override
    public RegularAndStaticColumns columns()
    {
        return regularAndStaticColumns;
    }

    @Override
    public DeletionTime partitionLevelDeletion()
    {
        return partitionLevelDeletion;
    }

    public EncodingStats stats()
    {
        return input.stats();
    }

    @Override
    public boolean isEmpty()
    {
        return staticRow().isEmpty() && partitionLevelDeletion().isLive() && !hasNext();
    }
}
