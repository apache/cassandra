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
package org.apache.cassandra.replication;

import java.util.Arrays;
import java.util.Collection;

public class Participants
{
    private final int[] hosts;

    Participants(Collection<Integer> participants)
    {
        int i = 0;
        int[] hosts = new int[participants.size()];
        for (int host : participants) hosts[i++] = host;
        Arrays.sort(hosts);
        this.hosts = hosts;
    }

    int size()
    {
        return hosts.length;
    }

    int indexOf(int hostId)
    {
        int idx = Arrays.binarySearch(hosts, hostId);
        if (idx < 0)
            throw new IllegalArgumentException("Unknown host id " + hostId);
        return idx;
    }

    int get(int idx)
    {
        if (idx < 0 || idx >= hosts.length)
            throw new IllegalArgumentException("Out of bounds host idx " + idx);
        return hosts[idx];
    }
}
