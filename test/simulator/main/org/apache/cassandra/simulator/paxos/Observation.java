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

package org.apache.cassandra.simulator.paxos;

class Observation implements Comparable<Observation>
{
    final int id;
    final Object[][] result;
    final int start;
    final int end;

    Observation(int id, Object[][] result, int start, int end)
    {
        this.id = id;
        this.result = result;
        this.start = start;
        this.end = end;
    }

    // computes a PARTIAL ORDER on when the outcome occurred, i.e. for many pair-wise comparisons the answer is 0
    public int compareTo(Observation that)
    {
        if (this.end < that.start)
            return -1;
        if (that.end < this.start)
            return 1;
        return 0;
    }
}
