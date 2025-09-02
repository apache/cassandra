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

package org.apache.cassandra.replication;

import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableId;

/**
 * See {@link UnreconciledMutations}.
 *
 * For now, all reads intersect with all transfers, but we could be more discerning and only return transfers ƒor the
 * specific table and range. Transfers should be very rare.
 */
public class UnreconciledTransfers
{
    private static final Logger logger = LoggerFactory.getLogger(UnreconciledTransfers.class);

    private final SortedSet<Integer> offsets = new TreeSet<>();

    public void activated(int offset)
    {
        logger.trace("Activating {}", offset);
        offsets.add(offset);
    }

    public boolean remove(int offset)
    {
        logger.trace("Removing {}", offset);
        return offsets.remove(offset);
    }

    void collect(Token token, TableId tableId, Offsets.OffsetReciever into)
    {
        logger.trace("Collecting offsets {}", offsets);
        offsets.forEach(into::add);
    }

    void collect(AbstractBounds<PartitionPosition> range, TableId tableId, Offsets.OffsetReciever into)
    {
        logger.trace("Collecting offsets {}", offsets);
        offsets.forEach(into::add);
    }
}
