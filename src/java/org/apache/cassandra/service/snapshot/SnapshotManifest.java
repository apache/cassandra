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

import java.io.*;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.JsonUtils;

// Only serialize fields
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY,
                getterVisibility = JsonAutoDetect.Visibility.NONE,
                setterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SnapshotManifest
{
    @JsonProperty("files")
    public final List<String> files;

    @JsonProperty("created_at")
    public final Instant createdAt;

    @JsonProperty("expires_at")
    public final Instant expiresAt;

    @JsonProperty("ephemeral")
    public final boolean ephemeral;

    /** needed for jackson serialization */
    @SuppressWarnings("unused")
    private SnapshotManifest()
    {
        this.files = null;
        this.createdAt = null;
        this.expiresAt = null;
        this.ephemeral = false;
    }

    public SnapshotManifest(List<String> files, DurationSpec.IntSecondsBound ttl, Instant creationTime, boolean ephemeral)
    {
        this.files = files;
        this.createdAt = creationTime;
        this.expiresAt = ttl == null ? null : createdAt.plusSeconds(ttl.toSeconds());
        this.ephemeral = ephemeral;
    }

    public List<String> getFiles()
    {
        return files;
    }

    public Instant getCreatedAt()
    {
        return createdAt;
    }

    public Instant getExpiresAt()
    {
        return expiresAt;
    }

    public boolean isEphemeral()
    {
        return ephemeral;
    }

    public void serializeToJsonFile(File outputFile) throws IOException
    {
        JsonUtils.serializeToJsonFile(this, outputFile);
    }

    public static SnapshotManifest deserializeFromJsonFile(File file) throws IOException
    {
        return JsonUtils.deserializeFromJsonFile(SnapshotManifest.class, file);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SnapshotManifest manifest = (SnapshotManifest) o;
        return Objects.equals(files, manifest.files)
               && Objects.equals(createdAt, manifest.createdAt)
               && Objects.equals(expiresAt, manifest.expiresAt)
               && Objects.equals(ephemeral, manifest.ephemeral);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(files, createdAt, expiresAt, ephemeral);
    }
}
