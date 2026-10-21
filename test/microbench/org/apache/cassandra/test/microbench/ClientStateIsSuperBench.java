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

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.auth.RoleOptions;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.Roles;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.TestHelper;

/**
 * Benchmark of the superuser check performed by {@link ClientState#isSuper()}, which every guardrail evaluation
 * goes through via {@link ClientState#isOrdinaryUser()} and is therefore executed several times per query.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = { "-Xmx512M", "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor" })
@Threads(4)
@State(Scope.Benchmark)
public class ClientStateIsSuperBench extends CQLTester
{
    private static final String USER_NAME = "bench_user";

    // long enough for the roles cache entry (roles_validity) and the memoized status to be alive during the test run
    private static final int ROLES_VALIDITY_MILLIS = 300_000;

    @Param({ "1", "8"})
    public int grantedRoles;

    private AuthenticatedUser user;
    private ClientState clientState;

    @Setup(Level.Trial)
    public void setup()
    {
        CQLTester.setUpClass();
        // set before the auth caches are built so that the roles cache picks the validity up
        DatabaseDescriptor.setRolesValidity(ROLES_VALIDITY_MILLIS);
        CQLTester.requireAuthentication();

        IRoleManager roleManager = DatabaseDescriptor.getRoleManager();

        RoleOptions withLogin = new RoleOptions();
        withLogin.setOption(IRoleManager.Option.LOGIN, true);
        RoleResource primaryRole = RoleResource.role(USER_NAME);
        roleManager.createRole(AuthenticatedUser.ANONYMOUS_USER, primaryRole, withLogin);

        RoleResource[] granted = new RoleResource[grantedRoles];
        for (int i = 0; i < grantedRoles; i++)
        {
            granted[i] = RoleResource.role("bench_granted_role_" + i);
            roleManager.createRole(AuthenticatedUser.ANONYMOUS_USER, granted[i], new RoleOptions());
        }
        AuthTestUtils.grantRolesTo(roleManager, primaryRole, granted);

        // rebuilds the cache with the validity set above, in case it was already built with the configured default
        Roles.cache.invalidate();
        // warm the entry so that the first measured call is a cache hit like all the following ones
        Roles.hasSuperuserStatus(primaryRole);

        user = new AuthenticatedUser(USER_NAME);
        clientState = ClientState.forExternalCalls(new InetSocketAddress("127.0.0.1", 9042));
        clientState.login(user);
    }

    @TearDown(Level.Trial)
    public void teardown() throws Exception
    {
        TestHelper.teardown();
    }

    @Benchmark
    public boolean clientStateIsSuper()
    {
        return clientState.isSuper();
    }

    @Benchmark
    public boolean authenticatedUserIsSuper()
    {
        return user.isSuper();
    }
}
