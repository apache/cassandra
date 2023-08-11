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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.cql3.CIDR;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.UnauthorizedException;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmark CIDR authorizer (enforce mode)
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 200, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = { "-Xmx512M", "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor" })
@Threads(4) // make sure this matches the number of _physical_cores_
@State(Scope.Benchmark)
public class CIDRAuthorizerBench extends CQLTester
{
    static Random random = new Random();

    static Map<String, List<String>> usersList = new HashMap<>();
    static List<InetAddress> ipList = new ArrayList<>();
    static Map<String, List<CIDR>> cidrsMapping = new HashMap<>();

    @Setup(Level.Trial)
    public void setup() throws IOException
    {
        CQLTester.setUpClass();
        CQLTester.requireAuthentication();

        byte[] ipParts = new byte[4];
        int numEntries = 100;

        while (numEntries > 0)
        {
            ipParts[0] = (byte) random.nextInt(256);
            ipParts[1] = (byte) random.nextInt(256);
            ipParts[2] = (byte) random.nextInt(256);
            InetAddress ip = InetAddress.getByAddress(ipParts);
            ipList.add(ip);

            String cidrGroupName = "cidrGroup" + numEntries;
            cidrsMapping.put(cidrGroupName, Collections.singletonList(new CIDR(ip, (short)24)));

            String userName = "user" + numEntries;
            usersList.put(userName, Collections.singletonList(cidrGroupName));
            numEntries--;
        }

        AuthTestUtils.createUsersWithCidrAccess(usersList);
        AuthTestUtils.insertCidrsMappings(cidrsMapping);
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        CQLTester.cleanup();
    }

    @State(Scope.Thread)
    public static class StateForValidLogin
    {
        InetSocketAddress ipAddr;
        AuthenticatedUser user;

        @Setup
        public void setupStateForValidLogin() throws UnknownHostException
        {
            int randIndex = random.nextInt(usersList.size());

            ipAddr = new InetSocketAddress(ipList.get(randIndex), 0);
            user = new AuthenticatedUser("user" + randIndex);
        }
    }

    @State(Scope.Thread)
    public static class StateForInvalidLogin
    {
        InetSocketAddress ipAddr;
        AuthenticatedUser user;

        @Setup
        public void setupStateForInvalidLogin() throws UnknownHostException
        {
            int randIndex = random.nextInt(usersList.size());

            ipAddr = new InetSocketAddress(ipList.get(randIndex), 0);
            user = new AuthenticatedUser("user" + (randIndex == 0 ? randIndex + 1 : randIndex - 1));
        }
    }

    @Benchmark
    public void benchCidrAuthorizer_ValidLogin(StateForValidLogin state)
    {
        state.user.hasAccessFromIp(state.ipAddr);
    }

    @Benchmark
    public void benchCidrAuthorizer_InvalidLogin(StateForInvalidLogin state)
    {
        try
        {
            state.user.hasAccessFromIp(state.ipAddr);
        }
        catch (UnauthorizedException e)
        {
            // Exception expected for invalid login, do nothing here
        }
    }
}
