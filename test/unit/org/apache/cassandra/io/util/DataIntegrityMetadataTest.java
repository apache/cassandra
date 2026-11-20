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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import java.util.zip.CRC32;

import com.google.common.collect.ImmutableMap;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;

import static accord.utils.Property.qt;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class DataIntegrityMetadataTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void testNonNumericDigestValueFailsWithClearError() throws IOException
    {
        assertInvalidDigestValueFailsWithClearError("not-a-number");
    }

    @Test
    public void testEmptyDigestFileFailsWithClearError() throws IOException
    {
        assertInvalidDigestValueFailsWithClearError("");
    }

    @Test
    public void testRejectsABufferSizeBelowFourKiB()
    {
        assertThatThrownBy(() -> DatabaseDescriptor.setSequentialReadBufferSizeInKB(3)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testValidatesAnIntactFileAndDetectsAFlippedBit()
    {
        qt().withExamples(50).check(rs -> {
            int buffer = DatabaseDescriptor.getSequentialReadBufferSize();
            java.io.File directory = tempDir.newFolder();
            File dataFile = new File(new java.io.File(directory, "data.db"));
            File digestFile = new File(new java.io.File(directory, "data.crc32"));

            Map<String, Integer> lengths = ImmutableMap.of("empty", 0,
                                                           "oneByte", 1,
                                                           "oneBeforeBuffer", buffer - 1,
                                                           "buffer", buffer,
                                                           "oneAfterBuffer", buffer + 1,
                                                           "anyLength", rs.nextInt(1, 3 * 1024 * 1024 + 1));
            for (Map.Entry<String, Integer> length : lengths.entrySet())
            {
                byte[] data = new byte[length.getValue()];
                rs.nextBytes(data);
                CRC32 crc = new CRC32();
                crc.update(data);
                Files.write(dataFile.toPath(), data);
                Files.write(digestFile.toPath(), String.valueOf(crc.getValue()).getBytes(StandardCharsets.UTF_8));

                assertThatCode(() -> new DataIntegrityMetadata.FileDigestValidator(dataFile, digestFile).validate())
                .doesNotThrowAnyException();

                if (data.length == 0)
                    continue;

                int lastByte = data.length - 1;
                Map<String, Integer> offsets = ImmutableMap.of("firstByte", 0,
                                                               "lastByte", lastByte,
                                                               "lastByteOfFirstBuffer", Math.min(buffer - 1, lastByte),
                                                               "firstByteOfSecondBuffer", Math.min(buffer, lastByte),
                                                               "anyByte", rs.nextInt(data.length));
                for (Map.Entry<String, Integer> offset : offsets.entrySet())
                {
                    data[offset.getValue()] ^= 1;
                    Files.write(dataFile.toPath(), data);
                    assertThatThrownBy(() -> new DataIntegrityMetadata.FileDigestValidator(dataFile, digestFile).validate())
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("integrity check (digest) failed");
                    data[offset.getValue()] ^= 1;
                }
            }
        });
    }

    private void assertInvalidDigestValueFailsWithClearError(String digestContent) throws IOException
    {
        File dataFile = new File(tempDir.newFile("data.db"));
        Files.write(dataFile.toPath(), "some bytes".getBytes(StandardCharsets.UTF_8));

        File digestFile = new File(tempDir.newFile("data.crc32"));
        Files.write(digestFile.toPath(), digestContent.getBytes(StandardCharsets.UTF_8));
        assertEquals("digest file fixture doesn't contain what this test expects",
                     digestContent.getBytes(StandardCharsets.UTF_8).length, digestFile.length());

        assertThatThrownBy(() -> new DataIntegrityMetadata.FileDigestValidator(dataFile, digestFile).validate())
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Corrupted file: invalid digest value in " + digestFile)
        .hasCauseInstanceOf(NumberFormatException.class);
    }
}
