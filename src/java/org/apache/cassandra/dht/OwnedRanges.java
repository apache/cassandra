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
package org.apache.cassandra.dht;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.StorageMetrics;

public final class OwnedRanges
{
    private static final Logger logger = LoggerFactory.getLogger(OwnedRanges.class);

    private static final Comparator<Range<Token>> rangeComparator = (r1, r2) ->
    {
        int cmp = r1.left.compareTo(r2.left);

        return cmp == 0 ? r1.right.compareTo(r2.right) : cmp;
    };

    // the set of token ranges that this node is a replica for
    private final List<Range<Token>> ownedRanges;

    public OwnedRanges(Collection<Range<Token>> ownedRanges)
    {
        this.ownedRanges = Range.normalize(ownedRanges);
    }

    /**
     * Check that all ranges in a requested set are contained by those in the owned set. Used in several contexts, such
     * as validating StreamRequests in StreamSession & PrepareMessage and ValidationRequest in RepairMessageVerbHandler.
     * In those callers, we want to verify that the token ranges specified in some request from a peer are not outside
     * the ranges owned by the local node. There are 2 levels of response if invalid ranges are detected, controlled
     * by options in Config; logging the event and rejecting the request and either/neither/both of these options may be
     * enabled. If neither are enabled, we short ciruit and immediately return success without any further processing.
     * If either option is enabled and we do detect unowned ranges in the request, we increment a metric then take further
     * action depending on the config.
     *
     * @param requestedRanges the set of token ranges contained in a request from a peer
     * @param requestId an identifier for the peer request, to be used in logging (e.g. Stream or Repair Session #)
     * @param requestType description of the request type, to be used in logging (e.g. "prepare request" or "validation")
     * @param from the originator of the request
     * @return true if the request should be accepted (either because no checking was performed, invalid ranges were d
     *         identified but only the logging action is enabled, or because all request ranges were valid. Otherwise,
     *         returns false to indicate the request should be rejected.
     */
    public boolean validateRangeRequest(Collection<Range<Token>> requestedRanges, String requestId, String requestType, InetAddressAndPort from)
    {
        Collection<Range<Token>> unownedRanges = testRanges(requestedRanges);

        if (!unownedRanges.isEmpty())
        {
            StorageMetrics.totalOpsForInvalidToken.inc();
            logger.warn("[{}] Received {} from {} containing ranges {} outside valid ranges {}",
                        requestId,
                        requestType,
                        from,
                        unownedRanges,
                        ownedRanges);
            return false;
        }
        return true;
    }

    /**
     * Takes a collection of ranges and returns ranges from that collection that are not covered by the this node's owned ranges.
     *
     * This normalizes the range collections internally, so:
     * a) be cautious about using this in any hot path
     * b) any returned ranges may not be identical to those present. That is, the returned values are post-normalization.
     *
     * e.g Given two collections:
     *      { (0, 100], (100, 200] }
     *      { (90, 100], (100, 110], (110, 300] }
     * the normalized forms are:
     *      { (0, 200] }
     *      { (90, 300] }
     * and so the return value would be:
     *      { (90, 300] }
     * which is equivalent, but not strictly equal to any member of the original supplied collection.
     *
     * @param testedRanges collection of candidate ranges to be checked
     * @return the ranges in testedRanges which are not covered by the owned ranges
     */
    @VisibleForTesting
    Collection<Range<Token>> testRanges(final Collection<Range<Token>> testedRanges)
    {
        if (ownedRanges.isEmpty())
            return testedRanges;

        // now normalize the second and check coverage of its members in the normalized first collection
        return Range.normalize(testedRanges).stream().filter(requested ->
        {
            // Find the point at which the target range would insert into the superset
            int index = Collections.binarySearch(ownedRanges, requested, rangeComparator);

            // an index >= 0 means an exact match was found so we can definitely accept this range
            if (index >= 0)
                return false;

            // convert to an insertion point in the superset
            index = Math.abs(index) - 1;

            // target sorts before the last list item, so we only need to check that one
            if (index >= ownedRanges.size())
                return !ownedRanges.get(index - 1).contains(requested);

            // target sorts before the first list item, so we only need to check that one
            if (index == 0)
                return !ownedRanges.get(index).contains(requested);

            // otherwise, check if the range on either side of the insertion point wholly contains the target
            return !(ownedRanges.get(index - 1).contains(requested) || ownedRanges.get(index).contains(requested));
        }).collect(Collectors.toSet());
    }
}
