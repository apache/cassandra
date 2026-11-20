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

package org.apache.cassandra.io.util;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.zip.CRC32;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;

import static accord.utils.Property.qt;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(BMUnitRunner.class)
@BMRule(name = "Record the size of each read of a data file",
        targetClass = "ChannelProxy",
        targetMethod = "read(java.nio.ByteBuffer, long)",
        condition = "$0.filePath().endsWith(\"data.db\")",
        action = "org.apache.cassandra.io.util.SequentialReadBufferSizeTest.readSizes.add($1.remaining())")
public class SequentialReadBufferSizeTest
{
    private static final int MiB = 1024 * 1024;

    static final List<Integer> readSizes = new CopyOnWriteArrayList<>();

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private int originalBufferSizeKiB;

    @Before
    public void clearReads()
    {
        readSizes.clear();
        originalBufferSizeKiB = DatabaseDescriptor.getSequentialReadBufferSizeInKB();
    }

    @After
    public void restoreBufferSize()
    {
        DatabaseDescriptor.setSequentialReadBufferSizeInKB(originalBufferSizeKiB);
    }

    @Test
    public void testDigestValidationReadsInChunksOfFourKiBByDefault()
    {
        assertThat(DatabaseDescriptor.getSequentialReadBufferSizeInKB()).isEqualTo(4);
        qt().withExamples(10).check(rs -> {
            int sizeInMiB = rs.nextInt(1, 10);
            byte[] data = new byte[sizeInMiB * MiB];
            rs.nextBytes(data);
            CRC32 crc = new CRC32();
            crc.update(data);
            java.io.File directory = tempDir.newFolder();
            File dataFile = new File(new java.io.File(directory, "data.db"));
            Files.write(dataFile.toPath(), data);
            File digestFile = new File(new java.io.File(directory, "data.crc32"));
            Files.write(digestFile.toPath(), String.valueOf(crc.getValue()).getBytes(StandardCharsets.UTF_8));
            readSizes.clear();
            new DataIntegrityMetadata.FileDigestValidator(dataFile, digestFile).validate();
            assertThat(readSizes).hasSize(sizeInMiB * MiB / 4096).containsOnly(4096);
        });
    }

    @Test
    public void testDigestValidationReadsInChunksOfOneMiBOnceSetOverJmx()
    {
        StorageService.instance.setSequentialReadBufferInKB(1024);
        assertThat(StorageService.instance.getSequentialReadBufferInKB()).isEqualTo(1024);
        qt().withExamples(10).check(rs -> {
            int sizeInMiB = rs.nextInt(1, 10);
            byte[] data = new byte[sizeInMiB * MiB];
            rs.nextBytes(data);
            CRC32 crc = new CRC32();
            crc.update(data);
            java.io.File directory = tempDir.newFolder();
            File dataFile = new File(new java.io.File(directory, "data.db"));
            Files.write(dataFile.toPath(), data);
            File digestFile = new File(new java.io.File(directory, "data.crc32"));
            Files.write(digestFile.toPath(), String.valueOf(crc.getValue()).getBytes(StandardCharsets.UTF_8));
            readSizes.clear();
            new DataIntegrityMetadata.FileDigestValidator(dataFile, digestFile).validate();
            assertThat(readSizes).hasSize(sizeInMiB).containsOnly(MiB);
        });
    }
}
