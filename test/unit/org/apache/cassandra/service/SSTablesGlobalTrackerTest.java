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

package org.apache.cassandra.service;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;

import com.googlecode.concurrenttrees.common.Iterables;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.File;
import org.assertj.core.util.Files;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.Generate;

import static org.junit.Assert.assertEquals;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.lists;
import static org.quicktheories.generators.SourceDSL.strings;

public class SSTablesGlobalTrackerTest
{
    private static final int MAX_VERSION_LIST_SIZE = 10;
    private static final int MAX_UPDATES_PER_GEN = 100;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.clientInitialization();
    }

    /**
     * Ensures that the tracker properly maintains the set of versions in use.
     *
     * <p>Using 'Quick Theories', we generate a number of random sstables notification and validate that after each
     * update the tracker computes the proper set of versions in use (using a simplisitic model that keeps the set
     * of all sstables in use and maps it to the set of its versions).
     */
    @Test
    public void testUpdates()
    {
        qt().forAll(lists().of(updates()).ofSizeBetween(0, MAX_UPDATES_PER_GEN),
                    sstableFormatTypes())
            .checkAssert((updates, format) -> {
                SSTablesGlobalTracker tracker = new SSTablesGlobalTracker(format);
                Set<Descriptor> all = new HashSet<>();
                Set<Version> previous = Collections.emptySet();
                for (Update update : updates)
                {
                    update.applyTo(all);
                    boolean triggerUpdate = tracker.handleSSTablesChange(update.removed, update.added);
                    Set<Version> expectedInUse = versionAndTypes(all);
                    assertEquals(expectedInUse, tracker.versionsInUse());
                    assertEquals(!expectedInUse.equals(previous), triggerUpdate);
                    previous = expectedInUse;
                }
            });
    }

    private Set<Version> versionAndTypes(Set<Descriptor> descriptors)
    {
        return descriptors.stream().map(d -> d.version).collect(Collectors.toSet());
    }

    private Gen<String> keyspaces()
    {
        return Generate.pick(Arrays.asList("k1", "k2"));
    }

    private Gen<String> tables()
    {
        return Generate.pick(Arrays.asList("t1", "t2", "t3"));
    }

    private Gen<Integer> generations()
    {
        return integers().between(1, 20);
    }

    private Gen<Descriptor> descriptors()
    {
        return sstableFormatTypes().zip(keyspaces(),
                                        tables(),
                                        generations(),
                                        sstableVersionString(),
                                        (f, k, t, g, v) -> new Descriptor(v, new File(Files.currentFolder()), k, t, new SequenceBasedSSTableId(g), f));
    }

    private Gen<List<Descriptor>> descriptorLists(int minSize)
    {
        return lists().of(descriptors()).ofSizeBetween(minSize, MAX_VERSION_LIST_SIZE);
    }

    private Gen<SSTableFormat<?, ?>> sstableFormatTypes()
    {
        return Generate.pick(Iterables.toList(DatabaseDescriptor.getSSTableFormats().values()));
    }

    private Gen<String> sstableVersionString()
    {
        // We want to somewhat favor the current version, as that is technically more realistic so we generate it 50%
        // of the time, and generate something random 50% of the time.
        return Generate.constant(DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion().version)
                       .mix(strings().betweenCodePoints('a', 'z').ofLength(2));
    }

    private Gen<Update> updates()
    {
        // We want to avoid update that remove and add nothing, as those appear to be generated quite a bit are not
        // too useful. Yet, having one of removed/added be empty is actually something we want.
        Gen<Update> maybeEmptyRemoved = descriptorLists(0).zip(descriptorLists(1), Update::new);
        Gen<Update> maybeEmptyAdded = descriptorLists(1).zip(descriptorLists(0), Update::new);
        return maybeEmptyRemoved.mix(maybeEmptyAdded);
    }

    private static class Update
    {
        final List<Descriptor> removed;
        final List<Descriptor> added;

        Update(List<Descriptor> removed, List<Descriptor> added)
        {
            this.removed = removed;
            this.added = added;
        }

        void applyTo(Set<Descriptor> allSSTables)
        {
            allSSTables.removeAll(removed);
            allSSTables.addAll(added);
        }

        @Override
        public String toString()
        {
            return "Update{" +
                   "removed=" + removed +
                   ", added=" + added +
                   '}';
        }
    }
}
