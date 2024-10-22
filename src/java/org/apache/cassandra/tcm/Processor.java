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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import accord.utils.Invariants;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LogState;

public interface Processor
{
    /**
     * Method is _only_ responsible to commit the transformation to the cluster metadata. Implementers _have to ensure_
     * local visibility and enactment of the metadata!
     */
    default Commit.Result commit(Entry.Id entryId, Transformation transform, Epoch lastKnown)
    {
        // When the cluster is bounced, it may happen that regular nodes come up earlier than CMS nodes, or CMS
        // nodes come up and fail to finish the startup since other CMS nodes are not up yet, and therefore can not
        // submit the STARTUP message. This allows the bounces affecting majority of CMS nodes to finish successfully.
        if (transform.kind() == Transformation.Kind.STARTUP)
        {
            return commit(entryId, transform, lastKnown,
                          Retry.Deadline.retryIndefinitely(DatabaseDescriptor.getCmsAwaitTimeout().to(TimeUnit.NANOSECONDS),
                                                           TCMMetrics.instance.commitRetries));
        }

        return commit(entryId, transform, lastKnown,
                      Retry.Deadline.after(DatabaseDescriptor.getCmsAwaitTimeout().to(TimeUnit.NANOSECONDS),
                                           new Retry.Jitter(TCMMetrics.instance.commitRetries)));
    }

    Commit.Result commit(Entry.Id entryId, Transformation transform, Epoch lastKnown, Retry.Deadline retryPolicy);

    /**
     * Fetches log from CMS up to the highest currently known epoch.
     * <p>
     * After fetching, all items _at least_ up to returned epoch will be visible.
     *
     * This method deliberately does not necessitate passing an epoch, since it guarantees catching up to the _latest_
     * epoch. Users that require catching up to _at least_ some epoch need to guard this call with a check of whether
     * local epoch is already at that point.
     */
    default ClusterMetadata fetchLogAndWait()
    {
        return fetchLogAndWait(null); // wait for the highest possible epoch
    }
;
    default ClusterMetadata fetchLogAndWait(Epoch waitFor)
    {
        return fetchLogAndWait(waitFor,
                               Retry.Deadline.after(DatabaseDescriptor.getCmsAwaitTimeout().to(TimeUnit.NANOSECONDS),
                                                    new Retry.Jitter(TCMMetrics.instance.fetchLogRetries)));
    }

    ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry.Deadline retryPolicy);

    /**
     * Queries node's _local_ state. It is not guaranteed to be contiguous, but can be used for restoring CMS state/
     */
    LogState getLocalState(Epoch start, Epoch end, boolean includeSnapshot, Retry.Deadline retryPolicy);

    /**
     * Queries global log state.
     */
    LogState getLogState(Epoch start, Epoch end, boolean includeSnapshot, Retry.Deadline retryPolicy);

    /**
     * Reconstructs
     */
    default List<ClusterMetadata> reconstruct(Epoch lowEpoch, Epoch highEpoch, Retry.Deadline retryPolicy)
    {
        LogState logState = getLogState(lowEpoch, highEpoch, true, retryPolicy);
        if (logState.isEmpty()) return Collections.emptyList();
        List<ClusterMetadata> cms = new ArrayList<>(logState.entries.size());

        ClusterMetadata acc = logState.baseState;
        cms.add(acc);
        for (Entry entry : logState.entries)
        {
            Invariants.checkState(entry.epoch.isDirectlyAfter(acc.epoch), "%s should have been directly after %s", entry.epoch, acc.epoch);
            Transformation.Result res = entry.transform.execute(acc);
            assert res.isSuccess() : res.toString();
            acc = res.success().metadata;
            cms.add(acc);
        }
        return cms;
    }

}
