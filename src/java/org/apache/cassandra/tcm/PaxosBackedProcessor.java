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

package org.apache.cassandra.tcm;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.tcm.log.LogStorage;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static org.apache.cassandra.schema.DistributedMetadataLogKeyspace.tryCommit;

public class PaxosBackedProcessor extends AbstractLocalProcessor
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosBackedProcessor.class);

    public PaxosBackedProcessor(LocalLog log)
    {
        super(log);
    }

    @Override
    protected boolean tryCommitOne(Entry.Id entryId, Transformation transform,
                                   Epoch previousEpoch, Epoch nextEpoch,
                                   long previousPeriod, long nextPeriod, boolean sealPeriod)
    {
        return tryCommit(entryId, transform, previousEpoch, nextEpoch, previousPeriod, nextPeriod, sealPeriod);
    }

    @Override
    protected ClusterMetadata tryReplayAndWait()
    {
        ClusterMetadata metadata = log.waitForHighestConsecutive();

        Set<Replica> replicas = metadata.fullCMSMembersAsReplicas();

        // We can not use Paxos to catch-up a member of CMS ownership group, since that'd reduce availability,
        // so instead we allow CMS owners to catch up via inconsistent replay. In other words, from local log
        // of the majority of the CMS replicas.
        int blockFor = replicas.size() == 1 ? 1 : (replicas.size() / 2) + 1;
        CountDownLatch latch = CountDownLatch.newCountDownLatch(blockFor);
        List<Epoch> fetched = new CopyOnWriteArrayList<>();

        for (Replica replica : replicas)
        {
            // TODO: test applying LogStates from multiple responses
            if (replica.isSelf())
            {
                log.append(LogStorage.SystemKeyspace.getLogState(metadata.epoch));
                latch.decrement();
            }
            else
            {
                Message<FetchCMSLog> request = Message.out(Verb.TCM_FETCH_CMS_LOG_REQ,
                                                           new FetchCMSLog(metadata.epoch, false));

                MessagingService.instance().sendWithCallback(request, replica.endpoint(),
                                                             (RequestCallback<LogState>) msg -> {
                                                                 fetched.add(msg.payload.latestEpoch());
                                                                 log.append(msg.payload);
                                                                 latch.decrement();
                                                             });
            }
        }

        try
        {
            if (latch.awaitUninterruptibly(DatabaseDescriptor.getCmsAwaitTimeout().to(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))
                return log.waitForHighestConsecutive();
            else
                throw new ReadTimeoutException(ConsistencyLevel.QUORUM, blockFor - latch.count(), blockFor, false);
        }
        finally
        {
            fetched.stream()
                   .max(Epoch::compareTo)
                   .ifPresent(max -> TCMMetrics.instance.cmsLogEntriesFetched(metadata.epoch, max));
        }
    }
}