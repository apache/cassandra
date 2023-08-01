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

package org.apache.cassandra.service.snapshot;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.assertj.core.api.Assertions.assertThatIOException;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.utils.JsonUtils;

public class SnapshotManifestTest
{
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testDeserializeFromInvalidFile() throws IOException {
        File manifestFile = new File(tempFolder.newFile("invalid"));
        assertThatIOException().isThrownBy(
            () -> {
                SnapshotManifest.deserializeFromJsonFile(manifestFile);
            });

        FileOutputStreamPlus out = new FileOutputStreamPlus(manifestFile);
        out.write(1);
        out.write(2);
        out.write(3);
        out.close();
        assertThatIOException().isThrownBy(
            () -> SnapshotManifest.deserializeFromJsonFile(manifestFile));
    }

    @Test
    public void testDeserializeManifest() throws IOException
    {
        Map<String, Object> map = new HashMap<>();
        String createdAt = "2021-07-03T10:37:30Z";
        String expiresAt = "2021-08-03T10:37:30Z";
        map.put("created_at", createdAt);
        map.put("expires_at", expiresAt);
        map.put("files", Arrays.asList("db1", "db2", "db3"));

        ObjectMapper mapper = JsonUtils.JSON_OBJECT_MAPPER;
        File manifestFile = new File(tempFolder.newFile("manifest.json"));
        mapper.writeValue((OutputStream) new FileOutputStreamPlus(manifestFile), map);
        SnapshotManifest manifest = SnapshotManifest.deserializeFromJsonFile(manifestFile);

        assertThat(manifest.getExpiresAt()).isEqualTo(Instant.parse(expiresAt));
        assertThat(manifest.getCreatedAt()).isEqualTo(Instant.parse(createdAt));
        assertThat(manifest.getFiles()).contains("db1").contains("db2").contains("db3").hasSize(3);
    }

    @Test
    public void testOptionalFields() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("files", Arrays.asList("db1", "db2", "db3"));
        ObjectMapper mapper = JsonUtils.JSON_OBJECT_MAPPER;
        File manifestFile = new File(tempFolder.newFile("manifest.json"));
        mapper.writeValue((OutputStream) new FileOutputStreamPlus(manifestFile), map);
        SnapshotManifest manifest = SnapshotManifest.deserializeFromJsonFile(manifestFile);

        assertThat(manifest.getExpiresAt()).isNull();
        assertThat(manifest.getCreatedAt()).isNull();
        assertThat(manifest.getFiles()).contains("db1").contains("db2").contains("db3").hasSize(3);
    }

    @Test
    public void testIngoredFields() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("files", Arrays.asList("db1", "db2", "db3"));
        map.put("dummy", "dummy");
        ObjectMapper mapper = JsonUtils.JSON_OBJECT_MAPPER;
        File manifestFile = new File(tempFolder.newFile("manifest.json"));
        mapper.writeValue((OutputStream) new FileOutputStreamPlus(manifestFile), map);
        SnapshotManifest manifest = SnapshotManifest.deserializeFromJsonFile(manifestFile);
        assertThat(manifest.getFiles()).contains("db1").contains("db2").contains("db3").hasSize(3);
    }

    @Test
    public void testSerializeAndDeserialize() throws Exception {
        SnapshotManifest manifest = new SnapshotManifest(Arrays.asList("db1", "db2", "db3"), new DurationSpec.IntSecondsBound("2m"), Instant.ofEpochMilli(currentTimeMillis()), false);
        File manifestFile = new File(tempFolder.newFile("manifest.json"));

        manifest.serializeToJsonFile(manifestFile);
        manifest = SnapshotManifest.deserializeFromJsonFile(manifestFile);
        assertThat(manifest.getExpiresAt()).isNotNull();
        assertThat(manifest.getCreatedAt()).isNotNull();
        assertThat(manifest.getFiles()).contains("db1").contains("db2").contains("db3").hasSize(3);
    }
}
