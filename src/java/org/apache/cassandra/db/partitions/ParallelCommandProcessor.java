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

import java.util.List;

import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.Pair;

/**
 * An "processor" over a number of unfiltered partitions (i.e. partitions containing deletion information).
 *
 * Unlike {@link UnfilteredPartitionIterator}, this is designed to be used concurrently.
 *
 * Unlike UnfilteredPartitionIterator which requires single-threaded
 *    while (partitions.hasNext())
 *    {
 *      var part = partitions.next();
 *      ...
 *      part.close();
 *    }
 *
 * this one allows concurrency, like
 *    var commands = partitions.getUninitializedCommands();
 *    commands.parallelStream().forEach(tuple -> {
 *        var iter = partitions.commandToIterator(tuple.left(), tuple.right());
 *    }
 */
public interface ParallelCommandProcessor
{
    /**
     * Single-threaded call to get all commands and corresponding keys.
     *
     * @return the list of partition read commands.
     */
    List<Pair<PrimaryKey, SinglePartitionReadCommand>> getUninitializedCommands();

    /**
     * Get an iterator for a given command and key.
     * This method can be called concurrently for reulst of getUninitializedCommands().
     *
     * @param command
     * @param key
     * @return
     */
    UnfilteredRowIterator commandToIterator(PrimaryKey key, SinglePartitionReadCommand command);
}
