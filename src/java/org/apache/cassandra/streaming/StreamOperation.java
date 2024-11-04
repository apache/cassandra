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
package org.apache.cassandra.streaming;

public enum StreamOperation
{
    OTHER("Other", true, false, false), // Fallback to avoid null types when deserializing from string
    RESTORE_REPLICA_COUNT("Restore replica count", false, false, false), // Handles removeNode
    DECOMMISSION("Unbootstrap", false, true, false),
    RELOCATION("Relocation", false, true, false),
    BOOTSTRAP("Bootstrap", false, true, false),
    REBUILD("Rebuild", false, true, false),
    BULK_LOAD("Bulk Load", true, false, false),
    REPAIR("Repair", true, false, true);

    private final String description;
    private final boolean requiresViewBuild;
    private final boolean keepSSTableLevel;
    private final boolean requiresBarrierTransaction;

    /**
     * @param description The operation description
     * @param requiresViewBuild Whether this operation requires views to be updated if it involves a base table
     * @param requiresBarrierTransaction Requires barrier to ensure all data that was repaired is already committed by txn system
     */
    StreamOperation(String description, boolean requiresViewBuild, boolean keepSSTableLevel, boolean requiresBarrierTransaction)
    {
        this.description = description;
        this.requiresViewBuild = requiresViewBuild;
        this.keepSSTableLevel = keepSSTableLevel;
        this.requiresBarrierTransaction = requiresBarrierTransaction;
    }

    public static StreamOperation fromString(String text)
    {
        for (StreamOperation b : StreamOperation.values())
        {
            if (b.description.equalsIgnoreCase(text))
                return b;
        }

        return OTHER;
    }

    public String getDescription()
    {
        return description;
    }

    /**
     * Wether this operation requires views to be updated
     */
    public boolean requiresViewBuild()
    {
        return this.requiresViewBuild;
    }

    public boolean keepSSTableLevel()
    {
        return keepSSTableLevel;
    }

    public boolean requiresBarrierTransaction()
    {
        return requiresBarrierTransaction;
    }
}
