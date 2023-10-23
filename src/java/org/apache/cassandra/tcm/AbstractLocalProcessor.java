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

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.Replication;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.exceptions.ExceptionCode.SERVER_ERROR;

public abstract class AbstractLocalProcessor implements Processor
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosBackedProcessor.class);

    protected final LocalLog log;

    public AbstractLocalProcessor(LocalLog log)
    {
        this.log = log;
    }

    /**
     * Epoch returned by processor in the Result is _not_ guaranteed to be visible by the Follower by
     * the time when this method returns.
     */
    @Override
    public final Commit.Result commit(Entry.Id entryId, Transformation transform, final Epoch lastKnown, Retry.Deadline retryPolicy)
    {
        while (!retryPolicy.reachedMax())
        {
            ClusterMetadata previous = log.waitForHighestConsecutive();
            if (!previous.fullCMSMembers().contains(FBUtilities.getBroadcastAddressAndPort()))
                throw new IllegalStateException("Node is not a member of CMS anymore");
            Transformation.Result result;
            if (!CassandraRelevantProperties.TCM_ALLOW_TRANSFORMATIONS_DURING_UPGRADES.getBoolean() &&
                !transform.allowDuringUpgrades() &&
                previous.metadataSerializationUpgradeInProgress())
            {
                result = new Transformation.Rejected(ExceptionCode.INVALID, "Upgrade in progress, can't commit " + transform);
            }
            else
            {
                result = transform.execute(previous, false);
            }
            // If we got a rejection, it could be that _we_ are not aware of the highest epoch.
            // Just try to catch up to the latest distributed state.
            if (result.isRejected())
            {
                ClusterMetadata replayed = fetchLogAndWait(null, retryPolicy);

                // Retry if replay has changed the epoch, return rejection otherwise.
                if (!replayed.epoch.isAfter(previous.epoch))
                    return new Commit.Result.Failure(result.rejected().code, result.rejected().reason, true);

                continue;
            }

            try
            {
                Epoch nextEpoch = result.success().metadata.epoch;
                // If metadata applies, try committing it to the log
                boolean applied = tryCommitOne(entryId, transform,
                                               previous.epoch, nextEpoch,
                                               previous.period, previous.nextPeriod(),
                                               result.success().metadata.lastInPeriod);

                // Application here semantially means "succeeded in committing to the distributed log".
                if (applied)
                {
                    logger.info("Committed {}. New epoch is {}", transform, nextEpoch);
                    log.append(new Entry(entryId, nextEpoch, new Transformation.Executed(transform, result)));
                    log.awaitAtLeast(nextEpoch);

                    return new Commit.Result.Success(result.success().metadata.epoch,
                                                     toReplication(result.success(), entryId, lastKnown, transform));
                }
                else
                {
                    retryPolicy.maybeSleep();
                    // TODO: could also add epoch from mis-application from [applied].
                    fetchLogAndWait(null, retryPolicy);
                }
            }
            catch (Throwable e)
            {
                logger.error("Caught error while trying to perform a local commit", e);
                JVMStabilityInspector.inspectThrowable(e);
                retryPolicy.maybeSleep();
            }
        }
        return new Commit.Result.Failure(SERVER_ERROR, String.format("Could not perform commit after %d/%d tries. Time remaining: %dms",
                                                                     retryPolicy.tries, retryPolicy.maxTries,
                                                                     TimeUnit.NANOSECONDS.toMillis(retryPolicy.remainingNanos())), false);
    }

    private Replication toReplication(Transformation.Success success, Entry.Id entryId, Epoch lastKnown, Transformation transform)
    {
        Replication replication;
        if (lastKnown == null || lastKnown.isDirectlyBefore(success.metadata.epoch))
            replication = Replication.of(new Entry(entryId, success.metadata.epoch, transform));
        else
            replication = log.getCommittedEntries(lastKnown);

        assert !replication.isEmpty();
        return replication;
    }

    @Override
    public abstract ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry.Deadline retryPolicy);
    protected abstract boolean tryCommitOne(Entry.Id entryId, Transformation transform,
                                            Epoch previousEpoch, Epoch nextEpoch,
                                            long previousPeriod, long nextPeriod, boolean sealPeriod);
}