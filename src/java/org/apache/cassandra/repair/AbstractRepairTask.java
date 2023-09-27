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
package org.apache.cassandra.repair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;

public abstract class AbstractRepairTask implements RepairTask
{
    protected static final Logger logger = LoggerFactory.getLogger(AbstractRepairTask.class);

    protected final RepairCoordinator coordinator;
    protected final InetAddressAndPort broadcastAddressAndPort;
    protected final RepairOption options;
    protected final String keyspace;

    protected AbstractRepairTask(RepairCoordinator coordinator)
    {
        this.coordinator = Objects.requireNonNull(coordinator);
        this.broadcastAddressAndPort = coordinator.ctx.broadcastAddressAndPort();
        this.options = Objects.requireNonNull(coordinator.state.options);
        this.keyspace = Objects.requireNonNull(coordinator.state.keyspace);
    }

    private List<RepairSession> submitRepairSessions(TimeUUID parentSession,
                                                     boolean isIncremental,
                                                     ExecutorPlus executor,
                                                     List<CommonRange> commonRanges,
                                                     String... cfnames)
    {
        List<RepairSession> futures = new ArrayList<>(options.getRanges().size());

        for (CommonRange commonRange : commonRanges)
        {
            logger.info("Starting RepairSession for {}", commonRange);
            RepairSession session = coordinator.ctx.repair().submitRepairSession(parentSession,
                                                                                 commonRange,
                                                                                 keyspace,
                                                                                 options.getParallelism(),
                                                                                 isIncremental,
                                                                                 options.isPullRepair(),
                                                                                 options.getPreviewKind(),
                                                                                 options.optimiseStreams(),
                                                                                 options.repairPaxos(),
                                                                                 options.paxosOnly(),
                                                                                 executor,
                                                                                 cfnames);
            if (session == null)
                continue;
            session.addCallback(new RepairSessionCallback(session));
            futures.add(session);
        }
        return futures;
    }

    protected Future<CoordinatedRepairResult> runRepair(TimeUUID parentSession,
                                                        boolean isIncremental,
                                                        ExecutorPlus executor,
                                                        List<CommonRange> commonRanges,
                                                        String... cfnames)
    {
        List<RepairSession> allSessions = submitRepairSessions(parentSession, isIncremental, executor, commonRanges, cfnames);
        List<Collection<Range<Token>>> ranges = Lists.transform(allSessions, RepairSession::ranges);
        Future<List<RepairSessionResult>> f = FutureCombiner.successfulOf(allSessions);
        return f.map(results -> {
            logger.debug("Repair result: {}", results);
            return CoordinatedRepairResult.create(ranges, results);
        });
    }

    private class RepairSessionCallback implements FutureCallback<RepairSessionResult>
    {
        private final RepairSession session;

        public RepairSessionCallback(RepairSession session)
        {
            this.session = session;
        }

        @Override
        public void onSuccess(RepairSessionResult result)
        {
            String message = String.format("Repair session %s for range %s finished", session.getId(),
                                           session.ranges().toString());
            coordinator.notifyProgress(message);
        }

        @Override
        public void onFailure(Throwable t)
        {
            String message = String.format("Repair session %s for range %s failed with error %s",
                                           session.getId(), session.ranges().toString(), t.getMessage());
            coordinator.notifyError(new RuntimeException(message, t));
        }
    }
}
