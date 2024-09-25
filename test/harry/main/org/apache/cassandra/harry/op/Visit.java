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

import org.junit.Assert;

import org.apache.cassandra.harry.op.Operations.Operation;

public class Visit
{
    public final long lts;
    public final Operation[] operations;
    public final Set<Long> visitedPartitions;

    public final boolean selectOnly;
    public Visit(long lts, Operation[] operations)
    {
        Assert.assertTrue(operations.length > 0);
        this.lts = lts;
        this.operations = operations;
        this.visitedPartitions = new HashSet<>();
        boolean selectOnly = true;
        for (Operation operation : operations)
        {
            if (selectOnly && !(operation instanceof Operations.SelectStatement))
                selectOnly = false;

            if (operation instanceof Operations.PartitionOperation)
                visitedPartitions.add(((Operations.PartitionOperation) operation).pd());

        }
        this.selectOnly = selectOnly;
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