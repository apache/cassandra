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
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * A set of sstables that were picked for compaction along with some other relevant properties.
 * <p/>
 * This is a list of sstables that should be compacted together after having been picked by a compaction strategy,
 * for example from a bucket in {@link SizeTieredCompactionStrategy} or from a level in {@link LeveledCompactionStrategy}.
 * Also, it contains other useful parameters such as a score that was assigned to this candidate (the read hotness or level
 * score depending on the strategy) and the level, if applicable.
 **/
class CompactionPick
{
    final static CompactionPick EMPTY = create(-1, new CopyOnWriteArraySet<>(), 0);

    /** The key to the parent compaction aggregate, e.g. a level number or tier avg size, -1 if no parent */
    final long parent;

    /** The sstables to be compacted */
    final CopyOnWriteArraySet<SSTableReader> sstables;

    /** The sum of all the sstable hotness scores */
    final double hotness;

    /** The average size in bytes for the sstables in this compaction */
    final long avgSizeInBytes;

    /** The unique compaction id, this is only available when a compaction is submitted */
    @Nullable
    volatile UUID id;

    /** The compaction progress, this is only available when compaction actually starts and will be null as long as
     * the candidate is still pending execution, also some tasks cannot report a progress at all, e.g. {@link SingleSSTableLCSTask}.
     * */
    @Nullable volatile CompactionProgress progress;

    /** Set to true when the compaction has completed */
    volatile boolean completed;

    private CompactionPick(long parent, Collection<SSTableReader> sstables, double hotness, long avgSizeInBytes)
    {
        this.parent = parent;
        this.sstables = new CopyOnWriteArraySet<>(sstables);
        this.hotness = hotness;
        this.avgSizeInBytes = avgSizeInBytes;
    }

    /**
     * Create a pending compaction candidate calculating hotness and avg size.
     */
    static CompactionPick create(long parent, Collection<SSTableReader> sstables)
    {
        return create(parent, sstables, CompactionAggregate.getTotHotness(sstables), CompactionAggregate.getAvgSizeBytes(sstables));
    }

    /**
     * Create a pending compaction candidate calculating avg size.
     */
    static CompactionPick create(long parent, Collection<SSTableReader> sstables, double hotness)
    {
        return create(parent, sstables, hotness, CompactionAggregate.getAvgSizeBytes(sstables));
    }

    /**
     * Create a pending compaction candidate with the given parameters.
     */
    static CompactionPick create(long parent, Collection<SSTableReader> sstables, double hotness, long avgSizeInBytes)
    {
        return new CompactionPick(parent, sstables, hotness, avgSizeInBytes);
    }

    /**
     * Create new compaction pick similar to the one provided but with a new parent.
     */
    static CompactionPick create(long parent, CompactionPick pick)
    {
        return new CompactionPick(parent, pick.sstables, pick.hotness, pick.avgSizeInBytes);
    }

    public double hotness()
    {
        return hotness;
    }

    public long avgSizeInBytes()
    {
        return avgSizeInBytes;
    }

    void setSubmitted(UUID id)
    {
        if (id == null)
            throw new IllegalArgumentException("Id cannot be null");

        if (this.id != null)
            throw new IllegalStateException("Already submitted");

        this.id = id;
    }
    /**
     * Set the compaction progress, this means the compaction pick has started executing.
     */
    void setProgress(CompactionProgress progress)
    {
        if (progress == null)
            throw new IllegalArgumentException("Progress cannot be null");

        if (this.progress != null)
            throw new IllegalStateException("Already compacting");

        if (this.id == null)
            setSubmitted(progress.operationId());
        else if (this.id != progress.operationId())
            throw new IllegalStateException("Submitted with a different id");

        this.progress = progress;
    }

    void setCompleted()
    {
        if (this.completed)
            throw new IllegalStateException("Already completed");

        this.completed = true;
    }

    /**
     * Add more sstables to the collection of sstables initially picked.
     * <p/>
     * This is currently used by {@link TimeWindowCompactionStrategy} to add expired sstables.
     *
     * @param sstables the sstables to add
     */
    CompactionPick withAddedSSTables(Collection<SSTableReader> sstables)
    {
        ImmutableList.Builder builder = ImmutableList.builder();
        builder.addAll(this.sstables);
        builder.addAll(sstables);

        return new CompactionPick(parent, builder.build(), CompactionAggregate.getTotHotness(sstables), CompactionAggregate.getAvgSizeBytes(sstables));
    }

    /**
     * @return true if this compaction candidate is empty, that is it has no sstables to compact.
     */
    boolean isEmpty()
    {
        return sstables.isEmpty();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(parent, sstables);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;

        if (!(obj instanceof CompactionPick))
            return false;

        CompactionPick that = (CompactionPick) obj;

        // a pick is the same if the sstables are the same given that the other properties are derived from sstables and two
        // picks are the same whether compaction has started or not so the progress and completed properties should not determine equality
        return parent == that.parent && sstables.equals(that.sstables);
    }

    @Override
    public String toString()
    {
        return String.format("Parent: %d, Hotness: %f, Avg size in bytes: %d, id: %s, sstables: %s", parent, hotness, avgSizeInBytes, id, sstables);
    }
}