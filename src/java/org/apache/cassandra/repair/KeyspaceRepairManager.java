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

package org.apache.cassandra.repair;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.BooleanSupplier;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.locator.RangesAtEndpoint;

/**
 * Keyspace level hook for repair.
 */
public interface KeyspaceRepairManager
{
    /**
     * Isolate the unrepaired ranges of the given tables, and make referenceable by session id. Until each table has
     * been notified that the repair session has been completed, the data associated with the given session id must
     * not be combined with repaired or unrepaired data, or data from other repair sessions.
     */
    ListenableFuture prepareIncrementalRepair(UUID sessionID,
                                              Collection<ColumnFamilyStore> tables,
                                              RangesAtEndpoint tokenRanges,
                                              ExecutorService executor,
                                              BooleanSupplier isCancelled);
}
