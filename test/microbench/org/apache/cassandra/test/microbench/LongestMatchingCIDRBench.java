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

package org.apache.cassandra.test.microbench;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.auth.CIDRGroupsMappingTable;
import org.apache.cassandra.cql3.CIDR;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmark algorithm(s) of the Longest matching CIDR for given IP
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 30, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = { "-Xmx512M", "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor" })
@Threads(4) // make sure this matches the number of _physical_cores_
@State(Scope.Benchmark)
public class LongestMatchingCIDRBench
{
    CIDRGroupsMappingTable<String> ipv4CidrGroupsMappingTable;
    static Random random = new Random();

    static List<String> cidrsToInsert = Arrays.asList("100.90.80.70/14",
                                                      "110.100.90.80/15",
                                                      "120.110.100.90/15",
                                                      "130.120.110.100/15",
                                                      "140.130.120.110/15",
                                                      "150.140.130.120/16",
                                                      "160.150.140.130/16",
                                                      "170.160.150.140/16",
                                                      "180.170.160.150/16",
                                                      "190.180.170.160/16",
                                                      "200.190.180.170/16",
                                                      "210.200.190.180/16",
                                                      "220.210.200.190/17",
                                                      "230.220.210.200/17",
                                                      "240.230.220.210/17",
                                                      "250.240.230.220/18",
                                                      "255.250.240.230/18");

    static List<String> ipsNotInserted = Arrays.asList("15.15.15.15",
                                                       "16.16.16.16",
                                                       "255.255.255.255",
                                                       "200.0.0.0",
                                                       "18.18.18.18",
                                                       "19.19.19.19",
                                                       "20.20.20.20",
                                                       "21.21.21.21",
                                                       "22.22.22.22",
                                                       "23.23.23.23",
                                                       "24.24.24.24",
                                                       "25.25.25.25",
                                                       "26.26.26.26",
                                                       "27.27.27.27",
                                                       "28.28.28.28",
                                                       "29.29.29.29",
                                                       "30.30.30.30");

    @Setup(Level.Trial)
    public void setup()
    {
        CIDRGroupsMappingTable.Builder<String> ipv4CidrGroupsMappingTableBuilder = CIDRGroupsMappingTable.builder(false);

        for (String cidr : cidrsToInsert)
        {
            ipv4CidrGroupsMappingTableBuilder.add(CIDR.getInstance(cidr), cidr);
        }
        ipv4CidrGroupsMappingTable = ipv4CidrGroupsMappingTableBuilder.build();
    }

    @State(Scope.Thread)
    public static class StateWithExistingCIDR
    {
        InetAddress ipAddr;

        @Setup
        public void setupStateWithExistingCidr() throws UnknownHostException
        {
            ipAddr = InetAddress.getByName(cidrsToInsert.get(random.nextInt(cidrsToInsert.size())).split("/")[0]);
        }
    }

    @State(Scope.Thread)
    public static class StateWithNonExistingCIDR
    {
        InetAddress ipAddr;

        @Setup
        public void setupStateWithNonExistingCidr() throws UnknownHostException
        {
            ipAddr = InetAddress.getByName(ipsNotInserted.get(random.nextInt(ipsNotInserted.size())));
        }
    }

    @Benchmark
    public void benchLongestMatchingCidr_CidrExistsCase(StateWithExistingCIDR state)
    {
        ipv4CidrGroupsMappingTable.lookupLongestMatchForIP(state.ipAddr);
    }

    @Benchmark
    public void benchLongestMatchingCidr_CidrNotExistsCase(StateWithNonExistingCIDR state)
    {
        ipv4CidrGroupsMappingTable.lookupLongestMatchForIP(state.ipAddr);
    }
}
