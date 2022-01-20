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
package org.apache.cassandra.repair;

import java.util.Collection;
import java.util.UUID;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.messages.RepairOption;

public interface RepairProgressReporter
{
    RepairProgressReporter instance = CassandraRelevantProperties.REPAIR_PROGRESS_REPORTER.isPresent()
                                      ? make(CassandraRelevantProperties.REPAIR_PROGRESS_REPORTER.getString())
                                      : new DefaultRepairProgressReporter();

    void onParentRepairStarted(UUID parentSession, String keyspaceName, String[] cfnames, RepairOption options);

    void onParentRepairSucceeded(UUID parentSession, Collection<Range<Token>> successfulRanges);

    void onParentRepairFailed(UUID parentSession, Throwable t);

    void onRepairsStarted(UUID id, UUID parentRepairSession, String keyspaceName, String[] cfnames, CommonRange commonRange);

    void onRepairsFailed(UUID id, String keyspaceName, String[] cfnames, Throwable t);

    void onRepairFailed(UUID id, String keyspaceName, String cfname, Throwable t);

    void onRepairSucceeded(UUID id, String keyspaceName, String cfname);

    static RepairProgressReporter make(String customImpl)
    {
        try
        {
            return (RepairProgressReporter) Class.forName(customImpl).newInstance();
        }
        catch (Throwable ex)
        {
            throw new IllegalStateException("Unknown repair progress report: " + customImpl);
        }
    }

    class DefaultRepairProgressReporter implements RepairProgressReporter
    {
        @Override
        public void onParentRepairStarted(UUID parentSession, String keyspaceName, String[] cfnames, RepairOption options)
        {
            SystemDistributedKeyspace.startParentRepair(parentSession, keyspaceName, cfnames, options);
        }

        @Override
        public void onParentRepairSucceeded(UUID parentSession, Collection<Range<Token>> successfulRanges)
        {
            SystemDistributedKeyspace.successfulParentRepair(parentSession, successfulRanges);
        }

        @Override
        public void onParentRepairFailed(UUID parentSession, Throwable t)
        {
            SystemDistributedKeyspace.failParentRepair(parentSession, t);
        }

        @Override
        public void onRepairsStarted(UUID id, UUID parentRepairSession, String keyspaceName, String[] cfnames, CommonRange commonRange)
        {
            SystemDistributedKeyspace.startRepairs(id, parentRepairSession, keyspaceName, cfnames, commonRange);
        }

        @Override
        public void onRepairsFailed(UUID id, String keyspaceName, String[] cfnames, Throwable t)
        {
            SystemDistributedKeyspace.failRepairs(id, keyspaceName, cfnames, t);
        }

        @Override
        public void onRepairFailed(UUID id, String keyspaceName, String cfname, Throwable t)
        {
            SystemDistributedKeyspace.failedRepairJob(id, keyspaceName, cfname, t);
        }

        @Override
        public void onRepairSucceeded(UUID id, String keyspaceName, String cfname)
        {
            SystemDistributedKeyspace.successfulRepairJob(id, keyspaceName, cfname);
        }
    }
}
