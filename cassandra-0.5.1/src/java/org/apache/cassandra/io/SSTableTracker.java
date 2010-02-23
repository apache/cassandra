package org.apache.cassandra.io;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


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
        assert sstable != null;
        assert sstable.getIndexPositions() != null;
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

    public synchronized void clearUnsafe()
    {
        sstables = Collections.emptySet();
    }
}
