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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public class CoordinatedRepairResult
{
    public final Collection<Range<Token>> successfulRanges;
    public final Collection<Range<Token>> failedRanges;
    public final Collection<Range<Token>> skippedRanges;
    public final Optional<List<RepairSessionResult>> results;

    public CoordinatedRepairResult(Collection<Range<Token>> successfulRanges,
                                   Collection<Range<Token>> failedRanges,
                                   Collection<Range<Token>> skippedRanges,
                                   List<RepairSessionResult> results)
    {
        this.successfulRanges = successfulRanges != null ? ImmutableList.copyOf(successfulRanges) : Collections.emptyList();
        this.failedRanges = failedRanges != null ? ImmutableList.copyOf(failedRanges) : Collections.emptyList();
        this.skippedRanges = skippedRanges != null ? ImmutableList.copyOf(skippedRanges) : Collections.emptyList();
        this.results = Optional.ofNullable(results);
    }

    public static CoordinatedRepairResult create(List<Collection<Range<Token>>> ranges, List<RepairSessionResult> results)
    {
        if (results == null || results.isEmpty())
            // something went wrong; assume all sessions failed
            return failed(ranges);

        assert ranges.size() == results.size() : String.format("range size %d != results size %d;ranges: %s, results: %s", ranges.size(), results.size(), ranges, results);
        Collection<Range<Token>> successfulRanges = new ArrayList<>();
        Collection<Range<Token>> failedRanges = new ArrayList<>();
        Collection<Range<Token>> skippedRanges = new ArrayList<>();
        int index = 0;
        for (RepairSessionResult sessionResult : results)
        {
            if (sessionResult != null)
            {
                // don't record successful repair if we had to skip ranges
                Collection<Range<Token>> replicas = sessionResult.skippedReplicas ? skippedRanges : successfulRanges;
                replicas.addAll(sessionResult.ranges);
            }
            else
            {
                // FutureCombiner.successfulOf doesn't keep track of the original, but maintains order, so
                // can fetch the original session
                failedRanges.addAll(Objects.requireNonNull(ranges.get(index)));
            }
            index++;
        }
        return new CoordinatedRepairResult(successfulRanges, failedRanges, skippedRanges, results);
    }

    private static CoordinatedRepairResult failed(@Nullable List<Collection<Range<Token>>> ranges)
    {
        Collection<Range<Token>> failedRanges = new ArrayList<>(ranges == null ? 0 : ranges.size());
        if (ranges != null)
            ranges.forEach(failedRanges::addAll);
        return new CoordinatedRepairResult(null, failedRanges, null, null);
    }

    /**
     * Utility method for tests to produce a success result; should only be used by tests as syntaxtic sugar as all
     * results must be present else an error is thrown.
     */
    @VisibleForTesting
    public static CoordinatedRepairResult success(List<RepairSessionResult> results)
    {
        assert results != null && results.stream().allMatch(a -> a != null) : String.format("results was null or had a null (failed) result: %s", results);
        List<Collection<Range<Token>>> ranges = Lists.transform(results, a -> a.ranges);
        return create(ranges, results);
    }

    public boolean hasFailed()
    {
        return !failedRanges.isEmpty();
    }
}
