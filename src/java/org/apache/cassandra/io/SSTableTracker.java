package org.apache.cassandra.io;

import java.util.*;
import java.io.IOException;


public class SSTableTracker implements Iterable<SSTableReader>
{
    private volatile Set<SSTableReader> sstables = Collections.emptySet();

    // TODO get rid of onstart crap.  this should really be part of the constructor,
    // but CFS isn't designed to set this up in the constructor, yet.
    public synchronized void onStart(Collection<SSTableReader> sstables)
    {
        this.sstables = Collections.unmodifiableSet(new HashSet<SSTableReader>(sstables));
    }

    public synchronized void add(SSTableReader sstable)
    {
        Set<SSTableReader> sstablesNew = new HashSet<SSTableReader>(sstables);
        sstablesNew.add(sstable);
        sstables = Collections.unmodifiableSet(sstablesNew);
    }

    // todo replace w/ compactionfinished for CASSANDRA-431
    public synchronized void markCompacted(Iterable<SSTableReader> compacted) throws IOException
    {
        Set<SSTableReader> sstablesNew = new HashSet<SSTableReader>(sstables);
        for (SSTableReader sstable : compacted)
        {
            sstablesNew.remove(sstable);
            sstable.markCompacted();
        }
        sstables = Collections.unmodifiableSet(sstablesNew);
    }

    // the modifiers create new, unmodifiable objects each time; the volatile fences the assignment
    // so we don't need any further synchronization for the common case here
    public Set<SSTableReader> getSSTables()
    {
        return sstables;
    }

    public int size()
    {
        return sstables.size();
    }

    public Iterator<SSTableReader> iterator()
    {
        return sstables.iterator();
    }
}
