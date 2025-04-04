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

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableId;

/**
 * Tracks unreconciled local mutations - the subset of all unreconciled mutations
 * that have been witnessed, or are currently being written to, on the local node.
 */
interface UnreconciledMutations
{
    void startWriting(Mutation mutation);

    void finishWriting(Mutation mutation);

    void remove(int offset);

    boolean collect(Token token, TableId tableId, boolean includePending, Offsets.OffsetReciever into);

    boolean collect(AbstractBounds<PartitionPosition> range, TableId tableId, boolean includePending, Offsets.OffsetReciever into);
}
