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

package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.TestDatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.File;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ZeroCopySSTableSplitterCompatibilityTest extends CQLTester
{
    private SSTableFormat<?, ?> originalFormat;

    @Before
    public void saveSSTableFormat()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
    }

    @After
    public void restoreSSTableFormat()
    {
        TestDatabaseDescriptor.setUnsafeSelectedSSTableFormat(originalFormat);
    }

    @Test
    public void compressedBtiSSTableIsRejected() throws Throwable
    {
        SSTableReader parent = compressedSSTable(BtiFormat.NAME);
        assertTrue(BtiFormat.is(parent.descriptor.getFormat()));
        assertFalse(parent.descriptor.version.hasSplitPrefixMarker());
        assertEquals(0, parent.getSSTableMetadata().firstPartitionPosition);
        assertFalse(ZeroCopySSTableSplitter.isSupported(parent));

        Set<String> before = fileNames(parent.descriptor.directory);
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.splitBySize(parent, 1024, null))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining(BtiFormat.NAME)
            .hasMessageContaining("BIG");
        assertEquals("a refused split must not create components", before, fileNames(parent.descriptor.directory));
    }

    @Test
    public void bigPrePaSSTableIsRejected() throws Throwable
    {
        SSTableReader current = compressedSSTable(BigFormat.NAME);
        assertTrue(BigFormat.is(current.descriptor.getFormat()));
        assertEquals("pa", current.descriptor.version.version);
        assertFalse(current.descriptor.version.hasSplitPrefixMarker());
        assertEquals("ordinary writer SSTables must retain the zero-position fast path",
                     0, current.getSSTableMetadata().firstPartitionPosition);
        assertTrue("the splitter must be able to upgrade a pa parent into marker-capable pb children",
                   ZeroCopySSTableSplitter.isSupported(current));

        Set<Component> components = new LinkedHashSet<>(current.descriptor.discoverComponents());
        Descriptor olderDescriptor = new Descriptor("oa",
                                                 current.descriptor.directory,
                                                 current.descriptor.ksname,
                                                 current.descriptor.cfname,
                                                 SSTableIdFactory.instance.defaultBuilder()
                                                                          .generator(Stream.empty()).get(),
                                                 BigFormat.getInstance());
        try
        {
            for (Component component : components)
            {
                Files.copy(current.descriptor.fileFor(component).toPath(),
                           olderDescriptor.fileFor(component).toPath());
            }

            SSTableReader olderReader = SSTableReader.open(getCurrentColumnFamilyStore(),
                                                           olderDescriptor,
                                                           components,
                                                           getCurrentColumnFamilyStore().metadata);
            try
            {
                assertFalse(olderReader.descriptor.version.hasSplitPrefixMarker());
                assertTrue(olderReader.compression);
                assertNull(olderReader.getCompressionMetadata().compressionDictionary());
                assertFalse(ZeroCopySSTableSplitter.isSupported(olderReader));

                Set<String> before = fileNames(olderDescriptor.directory);
                assertThatThrownBy(() -> ZeroCopySSTableSplitter.splitBySize(olderReader, 1024, null))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("oa")
                    .hasMessageContaining("pa-or-later");
                assertEquals("a refused split must not create components", before, fileNames(olderDescriptor.directory));
            }
            finally
            {
                olderReader.selfRef().release();
            }
        }
        finally
        {
            for (Component component : components)
                olderDescriptor.fileFor(component).deleteIfExists();
        }
    }

    @Test
    public void incompleteReaderComponentSetCannotHideCustomComponent() throws Throwable
    {
        SSTableReader parent = compressedSSTable(BigFormat.NAME);
        Set<Component> originalTOC = new LinkedHashSet<>(TOCComponent.loadTOC(parent.descriptor, false));
        Component custom = Component.parse("CustomZeroCopy.db", parent.descriptor.getFormat());
        try
        {
            assertTrue(parent.descriptor.fileFor(custom).createFileIfNotExists());
            Set<Component> updatedTOC = new LinkedHashSet<>(originalTOC);
            updatedTOC.add(custom);
            TOCComponent.rewriteTOC(parent.descriptor, updatedTOC);

            assertFalse("the already-open reader should model a caller that supplied an incomplete component set",
                        parent.getComponents().contains(custom));
            assertTrue(ZeroCopySSTableSplitter.unhandledComponents(parent).contains(custom));

            Set<String> before = fileNames(parent.descriptor.directory);
            assertThatThrownBy(() -> ZeroCopySSTableSplitter.splitBySize(parent, 1024, null))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(custom.name());
            assertEquals("a refused split must not create or remove components",
                         before, fileNames(parent.descriptor.directory));
        }
        finally
        {
            parent.descriptor.fileFor(custom).deleteIfExists();
            TOCComponent.rewriteTOC(parent.descriptor, originalTOC);
        }
    }

    private SSTableReader compressedSSTable(String format) throws Throwable
    {
        TestDatabaseDescriptor.setUnsafeSelectedSSTableFormat(format);
        createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}");
        disableCompaction();
        for (int partition = 0; partition < 40; partition++)
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)",
                    String.format("k%06d", partition), 0, "value");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader parent = cfs.getLiveSSTables().iterator().next();
        assertTrue(parent.compression);
        return parent;
    }

    private static Set<String> fileNames(File directory) throws IOException
    {
        return Arrays.stream(directory.tryList())
                     .map(File::name)
                     .collect(Collectors.toSet());
    }
}
