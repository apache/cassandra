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

package org.apache.cassandra.simulator.cluster;

import java.util.Arrays;

public class Topology
{
    public final int[] primaryKeys;
    public final int[] membersOfRing;
    public final long[] membersOfRingTokens;
    public final int[] membersOfQuorum;
    public final int[] rf;
    public final int quorumRf;
    public final int[][] replicasForKeys;
    public final int[][] pendingReplicasForKeys;

    public Topology(int[] primaryKeys, int[] membersOfRing, long[] membersOfRingTokens, int[] membersOfQuorum, int[] rf, int quorumRf, int[][] replicasForKeys, int[][] pendingReplicasForKeys)
    {
        for (int i = 0 ; i < primaryKeys.length ; ++i)
        {
            if (replicasForKeys[i].length != quorumRf)
                throw new AssertionError(String.format("Inconsistent ownership information: %s (expect %d)", Arrays.toString(replicasForKeys[i]), quorumRf));
        }
        this.primaryKeys = primaryKeys;
        this.membersOfRing = membersOfRing;
        this.membersOfRingTokens = membersOfRingTokens;
        this.membersOfQuorum = membersOfQuorum;
        this.rf = rf;
        this.quorumRf = quorumRf;
        this.replicasForKeys = replicasForKeys;
        this.pendingReplicasForKeys = pendingReplicasForKeys;
    }

    public int[] pendingKeys()
    {
        int count = 0;
        for (int i = 0 ; i < pendingReplicasForKeys.length ; ++i)
        {
            if (pendingReplicasForKeys[i].length > 0)
                ++count;
        }
        int[] pendingKeys = new int[count];
        count = 0;
        for (int i = 0 ; i < pendingReplicasForKeys.length ; ++i)
        {
            if (pendingReplicasForKeys[i].length == 0)
                continue;
            pendingKeys[count] = primaryKeys[i];
            count++;
        }
        return pendingKeys;
    }

    public Topology select(int[] selectPrimaryKeys)
    {
        int[][] newReplicasForKeys = new int[selectPrimaryKeys.length][];
        int[][] newPendingReplicasForKeys = new int[selectPrimaryKeys.length][];
        int in = 0, out = 0;
        while (out < newReplicasForKeys.length)
        {
            if (primaryKeys[in] < selectPrimaryKeys[out])
            {
                ++in;
                continue;
            }
            if (primaryKeys[in] > selectPrimaryKeys[out])
                throw new AssertionError();

            newReplicasForKeys[out] = replicasForKeys[in];
            newPendingReplicasForKeys[out] = pendingReplicasForKeys[in];
            ++in;
            ++out;
        }
        return new Topology(selectPrimaryKeys, membersOfRing, membersOfRingTokens, membersOfQuorum, rf, quorumRf,
                            newReplicasForKeys, newPendingReplicasForKeys);
    }
}