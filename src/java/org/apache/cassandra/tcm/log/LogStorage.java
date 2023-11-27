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

import org.apache.cassandra.tcm.Epoch;

public interface LogStorage extends LogReader
{
    void append(long period, Entry entry);
    LogState getLogState(Epoch since);

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
        public void append(long period, Entry entry) {}
        public LogState getLogState(Epoch since)
        {
            return LogState.EMPTY;
        }
        public Replication getReplication(Epoch since)
        {
            return Replication.EMPTY;
        }
        public Replication getReplication(long startPeriod, Epoch since)
        {
            return Replication.EMPTY;
        }
    }
}
