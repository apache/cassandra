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

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

import org.apache.cassandra.streaming.PreviewKind;

public class RepairSessionDesc
{
    public final UUID parentRepairSession;
    /** Repair session ID */
    public final UUID id;
    public final String keyspace;
    public final String[] cfnames;
    public final RepairParallelism parallelismDegree;
    public final boolean pullRepair;

    // indicates some replicas were not included in the repair. Only relevant for --force option
    public final boolean skippedReplicas;

    /** Range to repair */
    public final CommonRange commonRange;
    public final boolean isIncremental;
    public final PreviewKind previewKind;

    public final boolean optimiseStreams;

    public RepairSessionDesc(UUID parentRepairSession,
                             UUID id,
                             String keyspace,
                             String[] cfnames,
                             RepairParallelism parallelismDegree,
                             boolean pullRepair,
                             boolean skippedReplicas,
                             CommonRange commonRange,
                             boolean isIncremental,
                             PreviewKind previewKind,
                             boolean optimiseStreams)
    {
        this.parentRepairSession = parentRepairSession;
        this.id = id;
        this.keyspace = keyspace;
        this.cfnames = cfnames;
        this.parallelismDegree = parallelismDegree;
        this.pullRepair = pullRepair;
        this.skippedReplicas = skippedReplicas;
        this.commonRange = commonRange;
        this.isIncremental = isIncremental;
        this.previewKind = previewKind;
        this.optimiseStreams = optimiseStreams;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RepairSessionDesc that = (RepairSessionDesc) o;
        return pullRepair == that.pullRepair &&
               skippedReplicas == that.skippedReplicas &&
               isIncremental == that.isIncremental &&
               optimiseStreams == that.optimiseStreams &&
               Objects.equals(parentRepairSession, that.parentRepairSession) &&
               Objects.equals(id, that.id) &&
               Objects.equals(keyspace, that.keyspace) &&
               Arrays.equals(cfnames, that.cfnames) &&
               parallelismDegree == that.parallelismDegree &&
               Objects.equals(commonRange, that.commonRange) &&
               previewKind == that.previewKind;
    }

    public int hashCode()
    {
        int result = Objects.hash(parentRepairSession, id, keyspace, parallelismDegree, pullRepair, skippedReplicas, commonRange, isIncremental, previewKind, optimiseStreams);
        result = 31 * result + Arrays.hashCode(cfnames);
        return result;
    }
}
