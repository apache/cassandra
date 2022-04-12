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

package org.apache.cassandra.db.compaction;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.lifecycle.LifecycleTransaction;

/**
 * A set of sstables that were picked for compaction along with some other relevant properties.
 * <p/>
 * This is a list of sstables that should be compacted together after having been picked by a compaction strategy,
 * for example from a bucket in {@link SizeTieredCompactionStrategy} or from a level in {@link LeveledCompactionStrategy}.
 * Also, it contains other useful parameters such as a score that was assigned to this candidate (the read hotness or level
 * score depending on the strategy) and the level, if applicable.
 **/
public class CompactionPick
{
    final static CompactionPick EMPTY = create(-1, Collections.emptyList(), 0);

    /** The unique compaction id, this is available from the beginning and *MUST BE* used to create the transaction,
     * when it is submitted */
    private final UUID id;

    /** The key to the parent compaction aggregate, e.g. a level number or tier avg size, -1 if no parent */
    private final long parent;

    /** The sstables to be compacted */
    private final ImmutableSet<CompactionSSTable> sstables;

    /** Only expired sstables */
    private final ImmutableSet<CompactionSSTable> expired;

    /** The sum of all the sstable hotness scores */
    private final double hotness;

    /** The average size in bytes for the sstables in this compaction */
    private final long avgSizeInBytes;

    /** The total size on disk for the sstables in this compaction */
    private final long totSizeInBytes;

    /** This is set to true when the compaction is submitted */
    private volatile boolean submitted;

    /** The compaction progress, this is only available when compaction actually starts and will be null as long as
     * the candidate is still pending execution, also some tasks cannot report a progress at all, e.g. {@link SingleSSTableLCSTask}.
     * */
    @Nullable 
    private volatile CompactionProgress progress;

    /** Set to true when the compaction has completed */
    private volatile boolean completed;

    private CompactionPick(UUID id,
                           long parent,
                           Collection<? extends CompactionSSTable> compacting,
                           Collection<? extends CompactionSSTable> expired,
                           double hotness,
                           long avgSizeInBytes,
                           long totSizeInBytes)
    {
        this.id = Objects.requireNonNull(id);
        this.parent = parent;
        this.sstables = ImmutableSet.copyOf(compacting);
        this.expired = ImmutableSet.copyOf(expired);
        this.hotness = hotness;
        this.avgSizeInBytes = avgSizeInBytes;
        this.totSizeInBytes = totSizeInBytes;
    }

    /**
     * Create a pending compaction candidate with the given id, and average hotness and size.
     */
    public static CompactionPick create(UUID id,
                                        long parent,
                                        Collection<? extends CompactionSSTable> sstables,
                                        Collection<? extends CompactionSSTable> expired)
    {
        Collection<CompactionSSTable> nonExpiring = sstables.stream().filter(sstable -> !expired.contains(sstable)).collect(Collectors.toList());
        return create(id,
                      parent,
                      sstables,
                      expired,
                      CompactionAggregate.getTotHotness(nonExpiring),
                      CompactionAggregate.getAvgSizeBytes(nonExpiring),
                      CompactionAggregate.getTotSizeBytes(nonExpiring));
    }

    /**
     * Create a pending compaction candidate calculating hotness and avg and total size.
     */
    public static CompactionPick create(long parent, Collection<? extends CompactionSSTable> sstables, Collection<? extends CompactionSSTable> expired)
    {
        Collection<CompactionSSTable> nonExpiring = sstables.stream().filter(sstable -> !expired.contains(sstable)).collect(Collectors.toList());
        return create(LifecycleTransaction.newId(),
                      parent,
                      sstables,
                      expired,
                      CompactionAggregate.getTotHotness(nonExpiring),
                      CompactionAggregate.getAvgSizeBytes(nonExpiring),
                      CompactionAggregate.getTotSizeBytes(nonExpiring));
    }

    static CompactionPick create(long parent, Collection<? extends CompactionSSTable> sstables)
    {
        return create(parent, sstables, Collections.emptyList());
    }

    static CompactionPick create(UUID id, long parent, Collection<? extends CompactionSSTable> sstables)
    {
        return create(id, parent, sstables, Collections.emptyList());
    }

    /**
     * Create a pending compaction candidate calculating avg and total size.
     */
    static CompactionPick create(long parent, Collection<? extends CompactionSSTable> sstables, double hotness)
    {
        return create(LifecycleTransaction.newId(), parent, sstables, Collections.emptyList(), hotness, CompactionAggregate.getAvgSizeBytes(sstables), CompactionAggregate.getTotSizeBytes(sstables));
    }

    /**
     * Create a pending compaction candidate with the given parameters.
     */
    static CompactionPick create(UUID id,
                                 long parent,
                                 Collection<? extends CompactionSSTable> sstables,
                                 Collection<? extends CompactionSSTable> expired,
                                 double hotness,
                                 long avgSizeInBytes,
                                 long totSizeInBytes)
    {
        return new CompactionPick(id, parent, sstables, expired, hotness, avgSizeInBytes, totSizeInBytes);
    }

    public double hotness()
    {
        return hotness;
    }

    public long avgSizeInBytes()
    {
        return avgSizeInBytes;
    }

    public long totSizeInBytes()
    {
        return totSizeInBytes;
    }

    public long parent()
    {
        return parent;
    }

    public ImmutableSet<CompactionSSTable> sstables()
    {
        return sstables;
    }

    public ImmutableSet<CompactionSSTable> expired()
    {
        return expired;
    }

    public UUID id()
    {
        return id;
    }
    
    public CompactionProgress progress()
    {
        return progress;
    }
    
    public boolean completed()
    {
        return completed;
    }

    public boolean submitted() { return submitted; }

    public void setSubmitted(UUID id)
    {
        if (id == null || !this.id.equals(id))
            throw new IllegalArgumentException("Id should have been " + this.id);

        this.submitted = true;
    }
    /**
     * Set the compaction progress, this means the compaction pick has started executing.
     */
    public void setProgress(CompactionProgress progress)
    {
        if (progress == null)
            throw new IllegalArgumentException("Progress cannot be null");

        if (this.progress != null)
        {
            if (this.progress.operationId() == progress.operationId())
                return;
            else
                throw new IllegalStateException("Already compacting with different id");
        }

        if (!this.submitted())
            setSubmitted(progress.operationId());
        else if (this.id != progress.operationId())
            throw new IllegalStateException("Submitted with a different id");

        this.progress = progress;
    }

    public void setCompleted()
    {
        this.completed = true;
    }

    /**
     * Create new compaction pick similar to the one provided but with a new parent.
     */
    CompactionPick withParent(long parent)
    {
        return new CompactionPick(id,
                                  parent,
                                  sstables,
                                  expired,
                                  hotness,
                                  avgSizeInBytes,
                                  totSizeInBytes);
    }

    /**
     * Add more sstables to the collection of sstables initially picked.
     * <p/>
     * This is currently used by {@link TimeWindowCompactionStrategy} to add expired sstables.
     *
     * @param expired the sstables to add
     */
    CompactionPick withExpiredSSTables(Collection<CompactionSSTable> expired)
    {
        ImmutableSet<CompactionSSTable> newSSTables = ImmutableSet.<CompactionSSTable>builder()
                                                              .addAll(this.sstables)
                                                              .addAll(expired)
                                                              .build();
        ImmutableSet<CompactionSSTable> newExpired = ImmutableSet.<CompactionSSTable>builder()
                                                             .addAll(this.expired)
                                                             .addAll(expired)
                                                             .build();
        return new CompactionPick(id,
                                  parent,
                                  newSSTables,
                                  newExpired,
                                  hotness,
                                  avgSizeInBytes,
                                  totSizeInBytes);
    }

    /**
     * @return true if this compaction candidate is empty, that is it has no sstables to compact.
     */
    boolean isEmpty()
    {
        return sstables.isEmpty();
    }

    boolean hasExpiredOnly()
    {
        return sstables.size() == expired.size();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, parent, sstables, expired);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;

        if (!(obj instanceof CompactionPick))
            return false;

        CompactionPick that = (CompactionPick) obj;

        // a pick is the same if the sstables are the same given that
        // the other properties are derived from sstables and two
        // picks are the same whether compaction has started or not so
        // the progress and completed properties should not determine equality
        return id.equals(that.id)
               && parent == that.parent
               && sstables.equals(that.sstables)
               && expired.equals(that.expired);
    }

    @Override
    public String toString()
    {
        return String.format("Id: %s, Parent: %d, Hotness: %f, Avg size in bytes: %d, sstables: %s, expired: %s",
                             id,
                             parent,
                             hotness,
                             avgSizeInBytes,
                             sstables,
                             expired);
    }
}
