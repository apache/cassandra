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

import com.codahale.metrics.Meter;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.service.RetryStrategy;
import org.apache.cassandra.service.TimeoutStrategy;
import org.apache.cassandra.service.WaitStrategy;
import org.apache.cassandra.tcm.log.Entry;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.config.DatabaseDescriptor.getCmsAwaitTimeout;

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
            return commit(entryId, transform, lastKnown, unsafeRetryIndefinitely());
        }

        return commit(entryId, transform, lastKnown,
                      Retry.untilElapsed(getCmsAwaitTimeout().to(NANOSECONDS), TCMMetrics.instance.commitRetries));
    }

    /**
     * To be used only when submitting a STARTUP transformation when a node is restarted with a new set of addresses or
     * running a new release version.
     */
    static Retry unsafeRetryIndefinitely()
    {
        Meter retryMeter = TCMMetrics.instance.commitRetries;
        DurationSpec.IntMillisecondsBound defaultBackoff = DatabaseDescriptor.getDefaultRetryBackoff();
        DurationSpec.IntMillisecondsBound defaultMaxBackoff = DatabaseDescriptor.getDefaultMaxRetryBackoff();
        String spec = (defaultBackoff == null ? "100ms" : defaultBackoff.toMilliseconds() + "ms")
                      + "*attempts <=" + (defaultMaxBackoff == null ? "10s" : defaultMaxBackoff.toMilliseconds() + "ms");
        WaitStrategy wait = RetryStrategy.parse(spec, TimeoutStrategy.LatencySourceFactory.none());
        return Retry.withNoTimeLimit(retryMeter, wait);
    }

    Commit.Result commit(Entry.Id entryId, Transformation transform, Epoch lastKnown, Retry retryPolicy);

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
                               Retry.untilElapsed(getCmsAwaitTimeout().to(NANOSECONDS), TCMMetrics.instance.fetchLogRetries));
    }

    ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry retryPolicy);
}
