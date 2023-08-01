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
package org.apache.cassandra.io.sstable.format.bti;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.format.SortedTableVerifier;
import org.apache.cassandra.utils.OutputHandler;

public class BtiTableVerifier extends SortedTableVerifier<BtiTableReader> implements IVerifier
{
    public BtiTableVerifier(ColumnFamilyStore cfs, BtiTableReader sstable, OutputHandler outputHandler, boolean isOffline, Options options)
    {
        super(cfs, sstable, outputHandler, isOffline, options);
    }

    protected void verifyPartition(DecoratedKey key, UnfilteredRowIterator iterator)
    {
        // The trie writers abort if supplied with badly ordered or duplicate row keys. Verification is not necessary.
        // no-op, just open and close partition.
    }
}
