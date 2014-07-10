package org.apache.cassandra.stress.operations;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;

import org.apache.cassandra.stress.Operation;
import org.apache.cassandra.stress.generate.Distribution;

public class SampledOpDistribution implements OpDistribution
{

    final EnumeratedDistribution<Operation> operations;
    final Distribution clustering;
    private Operation cur;
    private long remaining;

    public SampledOpDistribution(EnumeratedDistribution<Operation> operations, Distribution clustering)
    {
        this.operations = operations;
        this.clustering = clustering;
    }

    public int maxBatchSize()
    {
        int max = 1;
        for (Pair<Operation, Double> pair : operations.getPmf())
            max = Math.max(max, (int) pair.getFirst().partitionCount.maxValue());
        return max;
    }

    public Operation next()
    {
        while (remaining == 0)
        {
            remaining = clustering.next();
            cur = operations.sample();
        }
        remaining--;
        return cur;
    }
}
