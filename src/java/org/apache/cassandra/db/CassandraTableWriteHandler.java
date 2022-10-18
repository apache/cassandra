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

package org.apache.cassandra.db;

import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.tracing.Tracing;

public class CassandraTableWriteHandler implements TableWriteHandler
{
    private final ColumnFamilyStore cfs;

    public CassandraTableWriteHandler(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
    }

    @Override
    @SuppressWarnings("resource")
    public void write(PartitionUpdate update, WriteContext context, boolean updateIndexes)
    {
        CassandraWriteContext ctx = CassandraWriteContext.fromContext(context);
        Tracing.trace("Adding to {} memtable", update.metadata().name);
        cfs.apply(update, ctx, updateIndexes);
    }
}
