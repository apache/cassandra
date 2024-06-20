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
package org.apache.cassandra.notifications;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.ColumnFamilyStore;

/**
 * Fired during truncate, after the memtable has been flushed but before any
 * snapshot is taken and SSTables are discarded
 */
public class TruncationNotification implements INotification
{
    public final ColumnFamilyStore cfs;
    public final boolean disableSnapshot;
    public final long truncatedAt;
    public final DurationSpec.IntSecondsBound ttl;

    public TruncationNotification(ColumnFamilyStore cfs,
                                  boolean disableSnapshot,
                                  long truncatedAt,
                                  DurationSpec.IntSecondsBound ttl)
    {
        this.cfs = cfs;
        this.disableSnapshot = disableSnapshot;
        this.truncatedAt = truncatedAt;
        this.ttl = ttl;
    }
}
