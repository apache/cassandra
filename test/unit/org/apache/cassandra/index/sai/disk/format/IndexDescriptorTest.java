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

package org.apache.cassandra.index.sai.disk.format;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IndexDescriptorTest
{
    private final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private Descriptor descriptor;

    @BeforeClass
    public static void initialise()
    {
        DatabaseDescriptor.toolInitialization();
    }

    @Before
    public void setup() throws Throwable
    {
        temporaryFolder.create();
        descriptor = Descriptor.fromFile(new File(temporaryFolder.newFolder().getAbsolutePath() + "/nb-1-big-Data.db"));
    }

    @After
    public void teardown() throws Throwable
    {
        temporaryFolder.delete();
    }

    @Test
    public void versionAAPerSSTableComponentIsParsedCorrectly() throws Throwable
    {
        createFileOnDisk("-SAI+aa+GroupComplete.db");

        IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor, Murmur3Partitioner.instance, SAITester.EMPTY_COMPARATOR);

        assertEquals(Version.AA, indexDescriptor.version);
        assertTrue(indexDescriptor.hasComponent(IndexComponent.GROUP_COMPLETION_MARKER));
    }

    @Test
    public void versionAAPerIndexComponentIsParsedCorrectly() throws Throwable
    {
        createFileOnDisk("-SAI+aa+test_index+ColumnComplete.db");

        IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor, Murmur3Partitioner.instance, SAITester.EMPTY_COMPARATOR);
        IndexIdentifier indexIdentifier = SAITester.createIndexIdentifier("test", "test", "test_index");

        assertEquals(Version.AA, indexDescriptor.version);
        assertTrue(indexDescriptor.hasComponent(IndexComponent.COLUMN_COMPLETION_MARKER, indexIdentifier));
    }

    private void createFileOnDisk(String filename) throws Throwable
    {
        Path path;
        try
        {
            path = Paths.get(URI.create(descriptor.baseFile() + filename));
        }
        catch (IllegalArgumentException ex)
        {
            path = Paths.get(descriptor.baseFile() + filename);
        }

        Files.touch(new File(path).toJavaIOFile());
    }
}
