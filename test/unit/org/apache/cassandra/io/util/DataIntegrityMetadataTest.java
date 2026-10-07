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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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
