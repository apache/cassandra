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

package org.apache.cassandra.db.compaction.differential;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The cursor path splits compaction output across data directories at the same partitions as the
 * iterator path, and every output holds only keys belonging to its directory.
 */
public class CursorDiskBoundaryDifferentialTest extends DifferentialCompactionTester
{
    private static final int DIRECTORIES = 3;
    private static final int PARTITIONS = 600;

    @Test
    public void diskBoundarySwitchMatchesIteratorOnBothPaths() throws Throwable
    {
        ColumnFamilyStore cfs = populated();
        ColumnFamilyStore bounded = withDirectories(cfs, boundaryPositions(cfs));

        assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), splitAcrossDirectories(bounded, true));
    }

    @Test
    public void committedOutputsRespectTheirDirectoryBoundaries() throws Throwable
    {
        ColumnFamilyStore cfs = populated();
        List<PartitionPosition> positions = boundaryPositions(cfs);
        ColumnFamilyStore bounded = withDirectories(cfs, positions);

        commitThroughFactory(cfs, true, splitAcrossDirectories(bounded, false));

        List<SSTableReader> outputs = new ArrayList<>(cfs.getLiveSSTables());
        assertEquals("one output per directory the keys span; DefaultCompactionWriter cannot split " +
                     "for any other reason, so a single output means the boundary path never ran",
                     DIRECTORIES, outputs.size());

        for (SSTableReader output : outputs)
        {
            int first = boundaryIndexOf(positions, output.getFirst());
            int last = boundaryIndexOf(positions, output.getLast());
            assertEquals("output " + output.descriptor.id + " spans a disk boundary: its first key " +
                         "belongs to directory " + first + " and its last to directory " + last,
                         first, last);

            // The output must physically sit in that directory.
            String expected = new File(bounded.getDirectories().getLocationForDisk(
                bounded.getDiskBoundaries().directories.get(first)).path()).absolutePath();
            assertTrue("output " + output.descriptor.id + " belongs to directory " + first +
                       " but was written to " + output.descriptor.directory.absolutePath(),
                       output.descriptor.directory.absolutePath().startsWith(expected));
        }
    }

    /** The index of the first boundary at or above this key: the directory the key belongs to. */
    private static int boundaryIndexOf(List<PartitionPosition> positions, DecoratedKey key)
    {
        for (int i = 0; i < positions.size(); i++)
            if (key.compareTo(positions.get(i)) <= 0)
                return i;
        return positions.size();
    }

    /** Boundaries that split the output into exactly {@link #DIRECTORIES} pieces. */
    private static List<PartitionPosition> boundaryPositions(ColumnFamilyStore cfs)
    {
        List<DecoratedKey> keys = new ArrayList<>(PARTITIONS);
        for (int pk = 0; pk < PARTITIONS; pk++)
            keys.add(cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(pk)));
        keys.sort(Comparator.naturalOrder());

        // maxKeyBound sits above every key of that token, so no key equals a boundary.
        List<PartitionPosition> positions = new ArrayList<>(DIRECTORIES);
        positions.add(keys.get(PARTITIONS / 3).getToken().maxKeyBound());
        positions.add(keys.get(2 * PARTITIONS / 3).getToken().maxKeyBound());
        positions.add(cfs.getPartitioner().getMaximumTokenForSplitting().maxKeyBound());
        return positions;
    }

    /** Builds a second view of the table with its own data directories and disk boundaries. */
    private static ColumnFamilyStore withDirectories(ColumnFamilyStore real, List<PartitionPosition> positions)
    {
        Directories.DataDirectory[] dirs = new Directories.DataDirectory[DIRECTORIES];
        for (int i = 0; i < DIRECTORIES; i++)
        {
            File dir = new File(DatabaseDescriptor.getAllDataFileLocations()[0],
                                "boundary-" + real.getTableName() + '-' + i);
            dir.tryCreateDirectories();
            dirs[i] = new Directories.DataDirectory(dir);
        }
        return new BoundedCFS(real, new Directories(real.metadata(), dirs), dirs, positions);
    }

    /** A view of one table carrying its own data directories and its own disk boundaries. */
    private static final class BoundedCFS extends ColumnFamilyStore
    {
        private final DiskBoundaries boundaries;

        BoundedCFS(ColumnFamilyStore real, Directories directories,
                   Directories.DataDirectory[] dirs, List<PartitionPosition> positions)
        {
            super(real.keyspace, real.getTableName(), Util.newSeqGen(), real.metadata.get(),
                  directories, false, false);
            this.boundaries = new DiskBoundaries(this, dirs, positions, Epoch.EMPTY, 0);
        }

        @Override
        public DiskBoundaries getDiskBoundaries()
        {
            // The superclass constructor calls this before the field is assigned.
            return boundaries == null ? super.getDiskBoundaries() : boundaries;
        }
    }

    /** A writer over the bounded view, so it splits across several directories. */
    private static TaskFactory splitAcrossDirectories(ColumnFamilyStore bounded, boolean retainOriginals)
    {
        return (cfs, txn, gcBefore) -> new CompactionTask(cfs, txn, gcBefore, retainOriginals)
        {
            @Override
            public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore ignored,
                                                                  Directories directories,
                                                                  ILifecycleTransaction transaction,
                                                                  Set<SSTableReader> nonExpiredSSTables)
            {
                return new DefaultCompactionWriter(bounded, bounded.getDirectories(), transaction,
                                                   nonExpiredSSTables, retainOriginals, 0);
            }
        };
    }

    private ColumnFamilyStore populated() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String padding = "x".repeat(200);
        for (int round = 0; round < 2; round++)
        {
            for (int pk = 0; pk < PARTITIONS; pk++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, round, padding);
            flush();
        }
        assertTrue("the fixture needs inputs", cfs.getLiveSSTables().size() >= 2);
        return cfs;
    }
}
