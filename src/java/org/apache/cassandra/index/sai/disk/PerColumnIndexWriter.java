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
package org.apache.cassandra.index.sai.disk;

import java.io.IOException;

import com.google.common.base.Stopwatch;

import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

/**
 * Creates the on-disk components for a given column index.
 */
public interface PerColumnIndexWriter
{
    /**
     * Adds a row to this index.
     */
    void addRow(PrimaryKey key, Row row, long sstableRowId) throws IOException;

    /**
     * Builds on-disk index data structures from accumulated data, moves them all to the filesystem, and fsync created files.
     */
    void complete(Stopwatch stopwatch) throws IOException;

    /**
     * Aborts accumulating data. Allows to clean up resources on error.
     * 
     * Note: Implementations should be idempotent, i.e. safe to call multiple times without producing undesirable side-effects.
     */
    void abort(Throwable cause);
}
