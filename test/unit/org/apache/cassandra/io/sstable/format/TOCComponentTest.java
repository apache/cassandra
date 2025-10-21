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
package org.apache.cassandra.io.sstable.format;

import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TOCComponentTest
{

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Before
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testAppendComponentsInSortedOrder() throws Exception
    {
        File dataFile = new File(tempDir.newFile("nb-1-big-Data.db"));
        File tocFile = new File(tempDir.getRoot().toPath().resolve("nb-1-big-TOC.txt"));
        Descriptor descriptor = Descriptor.fromFile(dataFile);
        Set<Component> components = Set.of(Components.DATA, Components.STATS, Components.DIGEST,
                                           Components.TOC, Components.COMPRESSION_INFO, Components.FILTER, Components.CRC);
        TOCComponent.updateTOC(descriptor, components);
        List<String> lines = Files.readAllLines(tocFile.toPath());
        assertThat(lines).containsExactly(
        "CRC.db",
        "CompressionInfo.db",
        "Data.db",
        "Digest.crc32",
        "Filter.db",
        "Statistics.db",
        "TOC.txt"
        );
    }

    @Test
    public void testUpdateTOCIdempotent() throws Exception
    {
        File dataFile = new File(tempDir.newFile("nb-1-big-Data.db"));
        File tocFile = new File(tempDir.getRoot().toPath().resolve("nb-1-big-TOC.txt"));
        Descriptor descriptor = Descriptor.fromFile(dataFile);
        Set<Component> components = Set.of(Components.DATA, Components.STATS, Components.FILTER);
        // call twice to check no duplication
        TOCComponent.updateTOC(descriptor, components);
        TOCComponent.updateTOC(descriptor, components);
        List<String> lines = Files.readAllLines(tocFile.toPath());
        assertThat(lines).containsExactly("Data.db", "Filter.db", "Statistics.db");
    }

    @Test(expected = FSWriteError.class)
    public void testUpdateTOCNonExistentDirectory()
    {
        File dataFile = new File(tempDir.getRoot(), "nonexistent/nb-1-big-Data.db");
        Descriptor descriptor = Descriptor.fromFile(dataFile);
        TOCComponent.updateTOC(descriptor, Set.of(Components.DATA));
    }

    @Test
    public void testLoadOrCreateSSTableExistButNotToc() throws Exception
    {
        java.io.File dataFile = tempDir.newFile("nb-1-big-Data.db");
        Descriptor descriptor = Descriptor.fromFile(new File(dataFile));

        Set<Component> components = TOCComponent.loadOrCreate(descriptor);
        assertThat(components).containsExactlyInAnyOrder(Components.DATA, Components.TOC);

        File tocFile = descriptor.fileFor(Components.TOC);
        assertTrue(tocFile.exists());
    }

    @Test
    public void testLoadOrCreateWithDiscoveredComponents() throws Exception
    {
        java.io.File dataFile = tempDir.newFile("nb-1-big-Data.db");
        tempDir.newFile("nb-1-big-Filter.db");

        Descriptor descriptor = Descriptor.fromFile(new File(dataFile));
        Set<Component> components = TOCComponent.loadOrCreate(descriptor);
        assertThat(components).containsExactlyInAnyOrder(Components.DATA, Components.FILTER, Components.TOC);

        File tocFile = descriptor.fileFor(Components.TOC);
        assertTrue(tocFile.exists());

        List<String> lines = Files.readAllLines(tocFile.toPath());
        assertThat(lines).containsExactly("Data.db", "Filter.db", "TOC.txt");
    }

    @Test
    public void testLoadOrCreateWhenNoComponents()
    {
        java.io.File dataFile = new java.io.File(tempDir.getRoot(), "nb-1-big-Data.db");
        Descriptor descriptor = Descriptor.fromFile(new File(dataFile));
        Set<Component> components = TOCComponent.loadOrCreate(descriptor);
        assertTrue(components.isEmpty());

        File tocFile = descriptor.fileFor(Components.TOC);
        assertFalse(tocFile.exists());
    }

    private Set<Component> loadTOC(boolean skipMissing) throws Exception
    {
        File dataFile = new File(tempDir.newFile("nb-1-big-Data.db"));
        File tocFile = new File(tempDir.newFile("nb-1-big-TOC.txt"));
        Descriptor desc = Descriptor.fromFile(dataFile);
        Files.write(tocFile.toPath(), List.of("Data.db", "Statistics.db"));
        return TOCComponent.loadTOC(desc, skipMissing);
    }

    @Test
    public void testSkipMissingFalse() throws Exception
    {
        assertThat(loadTOC(false)).containsExactlyInAnyOrder(Components.DATA, Components.STATS);
    }

    @Test
    public void testSkipMissingTrue() throws Exception
    {
        assertThat(loadTOC(true)).containsExactlyInAnyOrder(Components.DATA);
    }

    @Test
    public void testRewriteTOCReplacesFileWithSortedComponents() throws Exception
    {
        // Base file for descriptor
        Descriptor descriptor = Descriptor.fromFile(new File(tempDir.newFile("nb-1-big-Data.db")));

        File tocFile = descriptor.fileFor(Components.TOC);
        assertFalse(tocFile.exists());
        FileUtils.write(tocFile, Arrays.asList("Data-OLD.db", "Filter-OLD.db", "TOC-OLD.txt"));
        assertThat(FileUtils.readLines(tocFile)).containsExactly("Data-OLD.db", "Filter-OLD.db", "TOC-OLD.txt");

        Set<Component> newComponents = Set.of(Components.FILTER, Components.DATA, Components.TOC);
        TOCComponent.rewriteTOC(descriptor, newComponents);

        List<String> lines = Files.readAllLines(tocFile.toPath());
        assertThat(lines).containsExactly("Data.db", "Filter.db", "TOC.txt");
    }
}
