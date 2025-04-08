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

package org.apache.cassandra.harry.op;

import java.util.HashSet;
import java.util.Set;

import accord.utils.Invariants;
import org.apache.cassandra.harry.op.Operations.Operation;

public class Visit
{
    public final long lts;
    // TODO: specialize single-op visits
    public final Operation[] operations;
    public final long[] visitedPartitions;

    public final boolean validating;
    public final boolean hasCustom;

    public Visit(long lts, Operation[] operations)
    {
        Invariants.require(operations.length > 0);
        this.lts = lts;
        this.operations = operations;
        boolean selectOnly = true;
        boolean hasCustom = false;
        Set<Long> visitedPartitions = new HashSet<>();
        for (Operation operation : operations)
        {
            if (operation.kind() == Kind.CUSTOM)
                hasCustom = true;
            if (selectOnly && !(operation instanceof Operations.SelectStatement))
                selectOnly = false;

            if (operation instanceof Operations.PartitionOperation)
                visitedPartitions.add(((Operations.PartitionOperation) operation).pd());
        }
        this.visitedPartitions = new long[visitedPartitions.size()];
        int idx = 0;
        for (Long partition : visitedPartitions)
            this.visitedPartitions[idx++] = partition;
        this.validating = selectOnly;
        this.hasCustom = hasCustom;
    }

    public boolean validating()
    {
        return validating;
    }

    public String toString()
    {
        if (operations.length == 1)
            return String.format("Visit %d: %s", lts, operations[0]);

        StringBuilder sb = new StringBuilder();
        sb.append("Visit ").append(lts).append(":\n");
        boolean first = true;
        for (Operation operation : operations)
        {
            if (!first)
                sb.append("\n");
            first = false;
            sb.append(operation);
        }

        return sb.toString();
    }
}