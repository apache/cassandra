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
package org.apache.cassandra.db;

import java.time.Instant;
import java.io.File;
import java.io.IOException;
import org.apache.cassandra.io.util.FileUtils;
import java.util.Map;
import org.apache.cassandra.config.Duration;



public class SnapshotDetails {
    public String tag;
    public String keyspace;
    public String table;
    public Instant createdAt;
    public Instant expiresAt;
    public long sizeOnDiskBytes;
    public  long dataSizeBytes;

    public static final String CREATED_AT = "created_at";
    public static final String EXPIRES_AT = "expires_at";

    public SnapshotDetails(String tag, String table, String keyspace, Map<String, Object> manifest) {
        this.tag = tag;
        this.table = table;
        this.keyspace = keyspace;
        // Map<String, Object> manifest = FileUtils.readFileToJson(manifestFile);
        if (manifest.containsKey(CREATED_AT)) {
            this.createdAt = Instant.parse((String)manifest.get(CREATED_AT));
        }
        if (manifest.containsKey(EXPIRES_AT)) {
            this.expiresAt = Instant.parse((String)manifest.get(EXPIRES_AT));
        }
    }

    public boolean isExpired() {
        if (createdAt == null || expiresAt == null) {
            return false;
        }

        return expiresAt.compareTo(Instant.now()) < 0;
    }

    public boolean hasTTL() {
        return createdAt != null && expiresAt != null;
    }
}
