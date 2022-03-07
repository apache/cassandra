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

package org.apache.cassandra.db.compaction.writers;

import java.io.IOException;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

/**
 * Abstraction of compaction result writer, implemented by CompactionAwareWriter and tests.
 */
public interface SSTableDataSink
{
    /**
     * Append the given partition.
     * This is equivalent to a sequence of startPartition, addUnfiltered for each item in the partition, and endPartition.
     */
    boolean append(UnfilteredRowIterator partition);

    /**
     * Start a partition with the given key and deletion time.
     * Returns false if the partition could not be added (e.g. if the key is too long).
     */
    boolean startPartition(DecoratedKey partitionKey, DeletionTime deletionTime) throws IOException;

    /**
     * Complete a partition. Must be called once for every startPartition.
     */
    void endPartition() throws IOException;

    /**
     * Add a new row or marker in the current partition. Must be preceded by startPartition.
     */
    void addUnfiltered(Unfiltered unfiltered) throws IOException;
}
