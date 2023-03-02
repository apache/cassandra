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

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.DistributedMetadataLogKeyspace;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.LogState;
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
    public Commit.Result commit(Entry.Id entryId, Transformation transform, Epoch lastKnown)
    {
        return super.commit(entryId, transform, lastKnown);
    }

    @Override
    protected boolean tryCommitOne(Entry.Id entryId, Transformation transform,
                                   Epoch previousEpoch, Epoch nextEpoch,
                                   long previousPeriod, long nextPeriod, boolean sealPeriod)
    {
        return tryCommit(entryId, transform, previousEpoch, nextEpoch, previousPeriod, nextPeriod, sealPeriod);
    }

    @Override
    public ClusterMetadata replayAndWait()
    {
        log.waitForHighestConsecutive();
        ClusterMetadata metadata = log.metadata();

        Set<Replica> replicas = metadata.fullCMSMembersAsReplicas();

        // We can not use Paxos to catch-up a member of CMS ownership group, since that'd reduce availability,
        // so instead we allow CMS owners to catch up via inconsistent replay. In other words, from local log
        // of the majority of the CMS replicas.
        CountDownLatch latch = CountDownLatch.newCountDownLatch(replicas.size() == 1 ? 1 : (replicas.size() / 2) + 1);
        for (Replica replica : replicas)
        {
            // TODO: test applying LogStates from multiple responses
            if (replica.isSelf())
            {
                log.append(DistributedMetadataLogKeyspace.getLogState(metadata.epoch));
                latch.decrement();
            }
            else
            {
                Message<Replay> request = Message.out(Verb.TCM_REPLAY_REQ,
                                                      new Replay(metadata.epoch, false));

                MessagingService.instance().sendWithCallback(request, replica.endpoint(),
                                                             (RequestCallback<LogState>) msg -> {
                                                                 log.append(msg.payload);
                                                                 latch.decrement();
                                                             });
            }
        }

        // TODO: it is still possible to exit without catching up here
        latch.awaitUninterruptibly(10, TimeUnit.SECONDS);
        return log.waitForHighestConsecutive();
    }


}