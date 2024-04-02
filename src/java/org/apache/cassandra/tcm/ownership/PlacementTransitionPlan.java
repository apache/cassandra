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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;
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
    private static final Logger logger = LoggerFactory.getLogger(MovementMap.class);

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


    /**
     * Makes sure that a newly added read replica for a range already exists as a write replica
     *
     * We should never add both read & write replicas for the same range at the same time (or read replica before write)
     *
     * Also makes sure that we don't add a full read replica while the same write replica is only transient - we should
     * always make the write replica full before adding the read replica.
     *
     * We split and merge ranges, so in the previous placements we could have full write replicas (a, b], (b, c], but then
     * add a full read replica (a, c].
     *
     * @return null if everything is good, otherwise a Transformation.Result rejection containing information about the bad replica
     */
    @Nullable
    public void assertPreExistingWriteReplica(DataPlacements placements)
    {
        assertPreExistingWriteReplica(placements, toSplit, addToWrites(), moveReads(), removeFromWrites());
    }

    @Nullable
    @VisibleForTesting
    protected void assertPreExistingWriteReplica(DataPlacements placements, PlacementDeltas... deltasInOrder)
    {
        for (PlacementDeltas deltas : deltasInOrder)
        {
            for (Map.Entry<ReplicationParams, PlacementDeltas.PlacementDelta> entry : deltas)
            {
                ReplicationParams params = entry.getKey();
                PlacementDeltas.PlacementDelta delta = entry.getValue();
                for (Map.Entry<InetAddressAndPort, RangesAtEndpoint> addedRead : delta.reads.additions.entrySet())
                {
                    RangesAtEndpoint addedReadReplicas = addedRead.getValue();
                    RangesAtEndpoint existingWriteReplicas = placements.get(params).writes.byEndpoint().get(addedRead.getKey());
                    // we're adding read replicas - they should always exist as write replicas before doing that
                    // BUT we split and merge ranges so we need to check containment both ways
                    for (Replica newReadReplica : addedReadReplicas)
                    {
                        if (existingWriteReplicas.contains(newReadReplica))
                            continue;
                        boolean contained = false;
                        Set<Range<Token>> intersectingRanges = new HashSet<>();
                        for (Replica writeReplica : existingWriteReplicas)
                        {
                            if (writeReplica.isFull() == newReadReplica.isFull() || (writeReplica.isFull() && newReadReplica.isTransient()))
                            {
                                if (writeReplica.range().contains(newReadReplica.range()))
                                {
                                    contained = true;
                                    break;
                                }
                                else if (writeReplica.range().intersects(newReadReplica.range()))
                                {
                                    intersectingRanges.add(writeReplica.range());
                                }
                            }
                        }
                        if (!contained && Range.normalize(intersectingRanges).stream().noneMatch(writeReplica -> writeReplica.contains(newReadReplica.range())))
                        {
                            String message = "When adding a read replica, that replica needs to exist as a write replica before that: " + newReadReplica + '\n' + placements.get(params) + '\n' + delta;
                            logger.warn(message);
                            throw new Transformation.RejectedTransformationException(message);
                        }
                    }
                }
            }
            placements = deltas.apply(Epoch.FIRST, placements);
        }
    }
}
