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

package org.apache.cassandra.tcm.log;

import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;

public interface LogStorage extends LogReader
{
    void append(Entry entry);
    LogState getPersistedLogState();
    LogState getLogStateBetween(ClusterMetadata base, Epoch end);

    /**
     * We are using system keyspace even on CMS nodes (at least for now) since otherwise it is tricky
     * to implement replay from the log, given the distributed metadata log table is created not
     * during startup, but is initialized as a distributed table. We can, in theory, add an ability
     * to have distributed _system_ tables to avoid this duplication, but this doesn't seem to be
     * a priority now, especially given there's nothing that prevents us from truncating the log
     * table up to the last snapshot at any given time.
     */
    LogStorage SystemKeyspace = new SystemKeyspaceStorage();
    LogStorage None = new NoOpLogStorage();

    class NoOpLogStorage implements LogStorage
    {
        @Override
        public void append(Entry entry) {}

        @Override
        public LogState getLogState(Epoch startEpoch)
        {
            return LogState.EMPTY;
        }

        @Override
        public LogState getLogState(Epoch startEpoch, boolean allowSnapshots)
        {
            return LogState.EMPTY;
        }

        @Override
        public LogState getPersistedLogState()
        {
            return LogState.EMPTY;
        }

        @Override
        public EntryHolder getEntries(Epoch since)
        {
            return null;
        }

        @Override
        public MetadataSnapshots snapshots()
        {
            return null;
        }

        @Override
        public LogState getLogStateBetween(ClusterMetadata base, Epoch end)
        {
            return LogState.EMPTY;
        }
    }
}
