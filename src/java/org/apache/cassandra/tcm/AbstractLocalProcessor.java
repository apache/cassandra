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
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;
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
            {
                String msg = String.format("Node %s is not a CMS member in epoch %s; members=%s",
                                           FBUtilities.getBroadcastAddressAndPort(),
                                           previous.epoch.getEpoch(),
                                           previous.fullCMSMembers());
                logger.warn(msg);
                throw new NotCMSException(msg);
            }

            Transformation.Result result;
            if (!CassandraRelevantProperties.TCM_ALLOW_TRANSFORMATIONS_DURING_UPGRADES.getBoolean() &&
                !transform.allowDuringUpgrades() &&
                previous.metadataSerializationUpgradeInProgress())
            {
                result = new Transformation.Rejected(INVALID, "Upgrade in progress, can't commit " + transform);
            }
            else
            {
                result = executeStrictly(previous, transform);
            }

            // If we got a rejection, it could be that _we_ are not aware of the highest epoch.
            // Just try to catch up to the latest distributed state.
            if (result.isRejected())
            {
                ClusterMetadata replayed = fetchLogAndWait(null, retryPolicy);

                // Retry if replay has changed the epoch, return rejection otherwise.
                if (!replayed.epoch.isAfter(previous.epoch))
                {
                    return maybeFailure(entryId,
                                        lastKnown,
                                        () -> Commit.Result.rejected(result.rejected().code, result.rejected().reason, toLogState(lastKnown)));
                }

                continue;
            }

            try
            {
                Epoch nextEpoch = result.success().metadata.epoch;
                // If metadata applies, try committing it to the log
                boolean applied = tryCommitOne(entryId, transform, previous.epoch, nextEpoch);

                // Application here semantially means "succeeded in committing to the distributed log".
                if (applied)
                {
                    logger.info("Committed {}. New epoch is {}", transform, nextEpoch);
                    log.append(new Entry(entryId, nextEpoch, new Transformation.Executed(transform, result)));
                    log.awaitAtLeast(nextEpoch);

                    return new Commit.Result.Success(result.success().metadata.epoch,
                                                     toLogState(result.success(), entryId, lastKnown, transform));
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
        return Commit.Result.failed(SERVER_ERROR,
                                    String.format("Could not perform commit after %d/%d tries. Time remaining: %dms",
                                                  retryPolicy.tries, retryPolicy.maxTries,
                                                  TimeUnit.NANOSECONDS.toMillis(retryPolicy.remainingNanos())));
    }

    public Commit.Result maybeFailure(Entry.Id entryId, Epoch lastKnown, Supplier<Commit.Result.Failure> orElse)
    {
        LogState logState = toLogState(lastKnown);
        Epoch commitedAt = null;
        for (Entry entry : logState.entries)
        {
            if (entry.id.equals(entryId))
                commitedAt = entry.epoch;
        }

        // Succeeded after retry
        if (commitedAt != null)
            return new Commit.Result.Success(commitedAt, logState);
        else
            return orElse.get();
    }

    /**
     * Calls {@link Transformation#execute(ClusterMetadata)}, but catches any
     * {@link Transformation.RejectedTransformationException}, which is used by implementations to indicate that a known
     * and ultimately fatal error has been encountered. Throwing such an error implies that the transformation has not
     * succeeded and will not succeed if executed again, so no retries should be attempted. These scenarios are rare and
     * using an unchecked exception for this purpose enables us to propagate fatal errors without polluting the
     * {@link Transformation}interface . Here, those exceptions are caught and a {@link Transformation.Rejected}
     * response returned, shortcircuiting any retry logic intended to mitigate more transient failures.
     *
     * @param metadata the starting state
     * @param transformation to be applied to the starting state
     * @return result of the application
     */
    private Transformation.Result executeStrictly(ClusterMetadata metadata, Transformation transformation)
    {
        try
        {
            return transformation.execute(metadata);
        }
        catch (Transformation.RejectedTransformationException e)
        {
            return new Transformation.Rejected(INVALID, e.getMessage());
        }
    }


    private LogState toLogState(Transformation.Success success, Entry.Id entryId, Epoch lastKnown, Transformation transform)
    {
        if (lastKnown == null || lastKnown.isDirectlyBefore(success.metadata.epoch))
            return LogState.of(new Entry(entryId, success.metadata.epoch, transform));
        else
            return toLogState(lastKnown);
    }

    private LogState toLogState(Epoch lastKnown)
    {
        LogState logState;
        if (lastKnown == null)
            logState = LogState.EMPTY;
        else
        {
            // We can use local log here since we always call this method only if local log is up-to-date:
            // in case of a successful commit, we apply against latest metadata locally before committing,
            // and in case of a rejection, we fetch latest entries to verify linearizability.
            logState = log.getCommittedEntries(lastKnown);
        }

        return logState;
    }


    @Override
    public abstract ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry.Deadline retryPolicy);
    protected abstract boolean tryCommitOne(Entry.Id entryId, Transformation transform, Epoch previousEpoch, Epoch nextEpoch);

}