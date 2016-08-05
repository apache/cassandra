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
package org.apache.cassandra.db.lifecycle;

import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.collect.*;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Interval;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static org.apache.cassandra.db.lifecycle.Helpers.emptySet;
import static org.apache.cassandra.db.lifecycle.Helpers.filterOut;
import static org.apache.cassandra.db.lifecycle.Helpers.replace;

/**
 * An immutable structure holding the current memtable, the memtables pending
 * flush, the sstables for a column family, and the sstables that are active
 * in compaction (a subset of the sstables).
 *
 * Modifications to instances are all performed via a Function produced by the static methods in this class.
 * These are composed as necessary and provided to the Tracker.apply() methods, which atomically reject or
 * accept and apply the changes to the View.
 *
 */
public class View
{
    /**
     * ordinarily a list of size 1, but when preparing to flush will contain both the memtable we will flush
     * and the new replacement memtable, until all outstanding write operations on the old table complete.
     * The last item in the list is always the "current" memtable.
     */
    public final List<Memtable> liveMemtables;
    /**
     * contains all memtables that are no longer referenced for writing and are queued for / in the process of being
     * flushed. In chronologically ascending order.
     */
    public final List<Memtable> flushingMemtables;
    final Set<SSTableReader> compacting;
    final Set<SSTableReader> sstables;
    // we use a Map here so that we can easily perform identity checks as well as equality checks.
    // When marking compacting, we now  indicate if we expect the sstables to be present (by default we do),
    // and we then check that not only are they all present in the live set, but that the exact instance present is
    // the one we made our decision to compact against.
    final Map<SSTableReader, SSTableReader> sstablesMap;
    final Map<SSTableReader, SSTableReader> compactingMap;

    final SSTableIntervalTree intervalTree;

    View(List<Memtable> liveMemtables, List<Memtable> flushingMemtables, Map<SSTableReader, SSTableReader> sstables, Map<SSTableReader, SSTableReader> compacting, SSTableIntervalTree intervalTree)
    {
        assert liveMemtables != null;
        assert flushingMemtables != null;
        assert sstables != null;
        assert compacting != null;
        assert intervalTree != null;

        this.liveMemtables = liveMemtables;
        this.flushingMemtables = flushingMemtables;

        this.sstablesMap = sstables;
        this.sstables = sstablesMap.keySet();
        this.compactingMap = compacting;
        this.compacting = compactingMap.keySet();
        this.intervalTree = intervalTree;
    }

    public Memtable getCurrentMemtable()
    {
        return liveMemtables.get(liveMemtables.size() - 1);
    }

    /**
     * @return the active memtable and all the memtables that are pending flush.
     */
    public Iterable<Memtable> getAllMemtables()
    {
        return concat(flushingMemtables, liveMemtables);
    }

    // shortcut for all live sstables, so can efficiently use it for size, etc
    public Set<SSTableReader> liveSSTables()
    {
        return sstables;
    }

    public Iterable<SSTableReader> sstables(SSTableSet sstableSet, Predicate<SSTableReader> filter)
    {
        return filter(select(sstableSet), filter);
    }

    // any sstable known by this tracker in any form; we have a special method here since it's only used for testing/debug
    // (strong leak detection), and it does not follow the normal pattern
    @VisibleForTesting
    public Iterable<SSTableReader> allKnownSSTables()
    {
        return Iterables.concat(sstables, filterOut(compacting, sstables));
    }

    public Iterable<SSTableReader> select(SSTableSet sstableSet)
    {
        switch (sstableSet)
        {
            case LIVE:
                return sstables;
            case NONCOMPACTING:
                return filter(sstables, (s) -> !compacting.contains(s));
            case CANONICAL:
                Set<SSTableReader> canonicalSSTables = new HashSet<>();
                for (SSTableReader sstable : compacting)
                    if (sstable.openReason != SSTableReader.OpenReason.EARLY)
                        canonicalSSTables.add(sstable);
                // reason for checking if compacting contains the sstable is that if compacting has an EARLY version
                // of a NORMAL sstable, we still have the canonical version of that sstable in sstables.
                // note that the EARLY version is equal, but not == since it is a different instance of the same sstable.
                for (SSTableReader sstable : sstables)
                    if (!compacting.contains(sstable) && sstable.openReason != SSTableReader.OpenReason.EARLY)
                        canonicalSSTables.add(sstable);

                return canonicalSSTables;
            default:
                throw new IllegalStateException();
        }
    }

    public Iterable<SSTableReader> getUncompacting(Iterable<SSTableReader> candidates)
    {
        return filter(candidates, new Predicate<SSTableReader>()
        {
            public boolean apply(SSTableReader sstable)
            {
                return !compacting.contains(sstable);
            }
        });
    }

    public boolean isEmpty()
    {
        return sstables.isEmpty()
               && liveMemtables.size() <= 1
               && flushingMemtables.size() == 0
               && (liveMemtables.size() == 0 || liveMemtables.get(0).getOperations() == 0);
    }

    @Override
    public String toString()
    {
        return String.format("View(pending_count=%d, sstables=%s, compacting=%s)", liveMemtables.size() + flushingMemtables.size() - 1, sstables, compacting);
    }

    /**
     * Returns the sstables that have any partition between {@code left} and {@code right}, when both bounds are taken inclusively.
     * The interval formed by {@code left} and {@code right} shouldn't wrap.
     */
    public Iterable<SSTableReader> liveSSTablesInBounds(PartitionPosition left, PartitionPosition right)
    {
        assert !AbstractBounds.strictlyWrapsAround(left, right);

        if (intervalTree.isEmpty())
            return Collections.emptyList();

        PartitionPosition stopInTree = right.isMinimum() ? intervalTree.max() : right;
        return intervalTree.search(Interval.create(left, stopInTree));
    }

    public static List<SSTableReader> sstablesInBounds(PartitionPosition left, PartitionPosition right, SSTableIntervalTree intervalTree)
    {
        assert !AbstractBounds.strictlyWrapsAround(left, right);

        if (intervalTree.isEmpty())
            return Collections.emptyList();

        PartitionPosition stopInTree = right.isMinimum() ? intervalTree.max() : right;
        return intervalTree.search(Interval.create(left, stopInTree));
    }

    public static Function<View, Iterable<SSTableReader>> selectFunction(SSTableSet sstableSet)
    {
        return (view) -> view.select(sstableSet);
    }

    public static Function<View, Iterable<SSTableReader>> select(SSTableSet sstableSet, Predicate<SSTableReader> filter)
    {
        return (view) -> view.sstables(sstableSet, filter);
    }

    /**
     * @return a ViewFragment containing the sstables and memtables that may need to be merged
     * for the given @param key, according to the interval tree
     */
    public static Function<View, Iterable<SSTableReader>> select(SSTableSet sstableSet, DecoratedKey key)
    {
        assert sstableSet == SSTableSet.LIVE;
        return (view) -> view.intervalTree.search(key);
    }

    /**
     * @return a ViewFragment containing the sstables and memtables that may need to be merged
     * for rows within @param rowBounds, inclusive, according to the interval tree.
     */
    public static Function<View, Iterable<SSTableReader>> selectLive(AbstractBounds<PartitionPosition> rowBounds)
    {
        // Note that View.sstablesInBounds always includes it's bound while rowBounds may not. This is ok however
        // because the fact we restrict the sstables returned by this function is an optimization in the first
        // place and the returned sstables will (almost) never cover *exactly* rowBounds anyway. It's also
        // *very* unlikely that a sstable is included *just* because we consider one of the bound inclusively
        // instead of exclusively, so the performance impact is negligible in practice.
        return (view) -> view.liveSSTablesInBounds(rowBounds.left, rowBounds.right);
    }

    // METHODS TO CONSTRUCT FUNCTIONS FOR MODIFYING A VIEW:

    // return a function to un/mark the provided readers compacting in a view
    static Function<View, View> updateCompacting(final Set<SSTableReader> unmark, final Iterable<SSTableReader> mark)
    {
        if (unmark.isEmpty() && Iterables.isEmpty(mark))
            return Functions.identity();
        return new Function<View, View>()
        {
            public View apply(View view)
            {
                assert all(mark, Helpers.idIn(view.sstablesMap));
                return new View(view.liveMemtables, view.flushingMemtables, view.sstablesMap,
                                replace(view.compactingMap, unmark, mark),
                                view.intervalTree);
            }
        };
    }

    // construct a predicate to reject views that do not permit us to mark these readers compacting;
    // i.e. one of them is either already compacting, has been compacted, or has been replaced
    static Predicate<View> permitCompacting(final Iterable<SSTableReader> readers)
    {
        return new Predicate<View>()
        {
            public boolean apply(View view)
            {
                for (SSTableReader reader : readers)
                    if (view.compacting.contains(reader) || view.sstablesMap.get(reader) != reader || reader.isMarkedCompacted())
                        return false;
                return true;
            }
        };
    }

    // construct a function to change the liveset in a Snapshot
    static Function<View, View> updateLiveSet(final Set<SSTableReader> remove, final Iterable<SSTableReader> add)
    {
        if (remove.isEmpty() && Iterables.isEmpty(add))
            return Functions.identity();
        return new Function<View, View>()
        {
            public View apply(View view)
            {
                Map<SSTableReader, SSTableReader> sstableMap = replace(view.sstablesMap, remove, add);
                return new View(view.liveMemtables, view.flushingMemtables, sstableMap, view.compactingMap,
                                SSTableIntervalTree.build(sstableMap.keySet()));
            }
        };
    }

    // called prior to initiating flush: add newMemtable to liveMemtables, making it the latest memtable
    static Function<View, View> switchMemtable(final Memtable newMemtable)
    {
        return new Function<View, View>()
        {
            public View apply(View view)
            {
                List<Memtable> newLive = ImmutableList.<Memtable>builder().addAll(view.liveMemtables).add(newMemtable).build();
                assert newLive.size() == view.liveMemtables.size() + 1;
                return new View(newLive, view.flushingMemtables, view.sstablesMap, view.compactingMap, view.intervalTree);
            }
        };
    }

    // called before flush: move toFlush from liveMemtables to flushingMemtables
    static Function<View, View> markFlushing(final Memtable toFlush)
    {
        return new Function<View, View>()
        {
            public View apply(View view)
            {
                List<Memtable> live = view.liveMemtables, flushing = view.flushingMemtables;
                List<Memtable> newLive = copyOf(filter(live, not(equalTo(toFlush))));
                List<Memtable> newFlushing = copyOf(concat(filter(flushing, lessThan(toFlush)),
                                                           of(toFlush),
                                                           filter(flushing, not(lessThan(toFlush)))));
                assert newLive.size() == live.size() - 1;
                assert newFlushing.size() == flushing.size() + 1;
                return new View(newLive, newFlushing, view.sstablesMap, view.compactingMap, view.intervalTree);
            }
        };
    }

    // called after flush: removes memtable from flushingMemtables, and inserts flushed into the live sstable set
    static Function<View, View> replaceFlushed(final Memtable memtable, final Iterable<SSTableReader> flushed)
    {
        return new Function<View, View>()
        {
            public View apply(View view)
            {
                List<Memtable> flushingMemtables = copyOf(filter(view.flushingMemtables, not(equalTo(memtable))));
                assert flushingMemtables.size() == view.flushingMemtables.size() - 1;

                if (flushed == null || Iterables.isEmpty(flushed))
                    return new View(view.liveMemtables, flushingMemtables, view.sstablesMap,
                                    view.compactingMap, view.intervalTree);

                Map<SSTableReader, SSTableReader> sstableMap = replace(view.sstablesMap, emptySet(), flushed);
                return new View(view.liveMemtables, flushingMemtables, sstableMap, view.compactingMap,
                                SSTableIntervalTree.build(sstableMap.keySet()));
            }
        };
    }

    private static <T extends Comparable<T>> Predicate<T> lessThan(final T lessThan)
    {
        return new Predicate<T>()
        {
            public boolean apply(T t)
            {
                return t.compareTo(lessThan) < 0;
            }
        };
    }
}
