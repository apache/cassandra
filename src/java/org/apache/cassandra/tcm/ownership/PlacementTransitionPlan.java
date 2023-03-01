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

package org.apache.cassandra.tcm.ownership;

import org.apache.cassandra.tcm.sequences.LockedRanges;

/**
 *  A transition plan contains four elements:
 *  - deltas to split original ranges, if necessary
 *  - deltas to move each placement from start to maximal (i.e. overreplicated) state
 *  - deltas to move each placement from maximal to final state
 *  - deltas to merge final ranges, if necessary
 *  These deltas describe the abstract changes necessary to transition between two one cluster states.
 *  When the plan is compiled, these deltas are deconstructed and recomposed into the steps necessary for
 *  safe execution of the given operation.
 */
public class PlacementTransitionPlan
{
    public final PlacementDeltas toSplit;
    public final PlacementDeltas toMaximal;
    public final PlacementDeltas toFinal;
    public final PlacementDeltas toMerged;

    private PlacementDeltas addToWrites;
    private PlacementDeltas moveReads;
    private PlacementDeltas removeFromWrites;
    private LockedRanges.AffectedRanges affectedRanges;

    public PlacementTransitionPlan(PlacementDeltas toSplit,
                                   PlacementDeltas toMaximal,
                                   PlacementDeltas toFinal,
                                   PlacementDeltas toMerged)
    {
        this.toSplit = toSplit;
        this.toMaximal = toMaximal;
        this.toFinal = toFinal;
        this.toMerged = toMerged;
    }

    public PlacementDeltas addToWrites()
    {
        if (addToWrites == null)
            compile();
        return addToWrites;
    }
    public PlacementDeltas moveReads()
    {
        if (moveReads == null)
            compile();
        return moveReads;
    }

    public PlacementDeltas removeFromWrites()
    {
        if (removeFromWrites == null)
            compile();
        return removeFromWrites;
    }

    public LockedRanges.AffectedRanges affectedRanges()
    {
        if (affectedRanges == null)
            compile();
        return affectedRanges;
    }

    private void compile()
    {
        PlacementDeltas.Builder addToWrites = PlacementDeltas.builder();
        PlacementDeltas.Builder moveReads = PlacementDeltas.builder();
        PlacementDeltas.Builder removeFromWrites = PlacementDeltas.builder();
        LockedRanges.AffectedRangesBuilder affectedRanges = LockedRanges.AffectedRanges.builder();

        toSplit.forEach((replication, delta) -> {
            delta.reads.additions.flattenValues().forEach(r -> affectedRanges.add(replication, r.range()));
        });

        toMaximal.forEach((replication, delta) -> {
            delta.reads.additions.flattenValues().forEach(r -> affectedRanges.add(replication, r.range()));
            addToWrites.put(replication, delta.onlyWrites());
            moveReads.put(replication, delta.onlyReads());
        });

        toFinal.forEach((replication, delta) -> {
            delta.reads.additions.flattenValues().forEach(r -> affectedRanges.add(replication, r.range()));
            moveReads.put(replication, delta.onlyReads());
            removeFromWrites.put(replication, delta.onlyWrites());
        });

        toMerged.forEach((replication, delta) -> {
            delta.reads.additions.flattenValues().forEach(r -> affectedRanges.add(replication, r.range()));
            removeFromWrites.put(replication, delta);
        });
        this.addToWrites = addToWrites.build();
        this.moveReads = moveReads.build();
        this.removeFromWrites = removeFromWrites.build();
        this.affectedRanges = affectedRanges.build();

    }

    @Override
    public String toString()
    {
        return "PlacementTransitionPlan{" +
               "toSplit=" + toSplit +
               ", toMaximal=" + toMaximal +
               ", toFinal=" + toFinal +
               ", toMerged=" + toMerged +
               ", compiled=" + (addToWrites == null) +
               '}';
    }
}
