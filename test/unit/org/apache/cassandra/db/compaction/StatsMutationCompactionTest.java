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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.UUID;

import com.google.common.collect.Iterables;
import org.junit.Test;

import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;


public class StatsMutationCompactionTest extends AbstractPendingRepairTest
{
    @Test
    public void testStatsMutation() throws IOException
    {
        SSTableReader sstable = makeSSTable(false);
        long newRepairedAt = 0;
        UUID newPendingRepair = UUID.randomUUID();
        long statsLastModified = statsLastModified(sstable.descriptor);

        // prevent it from being removed by the transaction, so we can verify it
        Ref<SSTableReader> ref = sstable.ref();

        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstable, OperationType.ANTICOMPACTION))
        {
            StatsMutationCompaction.performStatsMutationCompaction(cfs, txn, stats -> stats.mutateRepairedMetadata(newRepairedAt, newPendingRepair, false));

            // verify original sstable is replaced by new sstable
            SSTableReader compacted = Iterables.getOnlyElement(cfs.getLiveSSTables());
            assertNotEquals(sstable, compacted);
            assertTrue(txn.originals().contains(sstable));
            assertTrue(txn.isObsolete(sstable));

            // verify stats metadata is modified
            assertEquals(newRepairedAt, compacted.getRepairedAt());
            assertEquals(newPendingRepair, compacted.getPendingRepair());

            // verify original stats stays immutable
            assertEquals(statsLastModified, statsLastModified(sstable.descriptor));
            assertEquals(sstable.components(), compacted.components());

            // verify all sstable components are hard links except for stats metadata.
            for (Component component : compacted.components())
            {
                boolean isHardLink = component != Component.STATS;
                assertEquals(isHardLink, isHardLink(sstable.descriptor, compacted.descriptor, component));

                long before = sstable.descriptor.fileFor(component).length();
                long after = compacted.descriptor.fileFor(component).length();

                if (isHardLink)
                    assertEquals(before, after);
                else
                    assertNotEquals(before, after);
            }
        }
        finally
        {
            ref.release();
        }
    }

    private static boolean isHardLink(Descriptor original, Descriptor compacted, Component component) throws IOException
    {
        return isHardLink(original.fileFor(component), compacted.fileFor(component));
    }

    private static boolean isHardLink(File original, File compacted) throws IOException
    {
        assert original.exists();
        assert compacted.exists();
        return Objects.equals(fileKey(original.toPath()), fileKey(compacted.toPath()));
    }

    private static Object fileKey(Path file) throws IOException
    {
        return Files.readAttributes(file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).fileKey();
    }

    private static long statsLastModified(Descriptor descriptor)
    {
        return descriptor.fileFor(Component.STATS).lastModified();
    }
}
