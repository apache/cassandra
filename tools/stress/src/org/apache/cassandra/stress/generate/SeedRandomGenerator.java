package org.apache.cassandra.stress.generate;
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


public class SeedRandomGenerator implements SeedGenerator
{

    final Distribution distribution;
    final Distribution clustering;

    private long next;
    private int count;

    public SeedRandomGenerator(Distribution distribution, Distribution clustering)
    {
        this.distribution = distribution;
        this.clustering = clustering;
    }

    public long next(long workIndex)
    {
        if (count == 0)
        {
            next = distribution.next();
            count = (int) clustering.next();
        }
        long result = next;
        count--;
        if (next == distribution.maxValue())
            next = distribution.minValue();
        else
            next++;
        return result;
    }
}
