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

package org.apache.cassandra.auth;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.UnauthorizedException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Property;
import accord.utils.RandomSource;
import org.apache.cassandra.utils.LoggingCommand;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.service.CassandraDaemon;

import static accord.utils.Property.commands;
import static accord.utils.Property.stateful;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Stateful property-based test verifying Cassandra's role graph acyclicity,
 * transitive role inheritance, and permission hierarchies against an in-memory oracle model.
 */
public class AuthorizationStatefulTest extends CQLTester
{
    private static final String KS = "auth_ks";
    private static final String TBL = "tbl";
    private static final String ISOLATED_TBL = "tbl_isolated";
    private static final String USER = "alice";
    private static final String PASSWORD = "alice_password";
    private static final String CLEAR_VAL = "sensitive";
    private static final String MASKED_VAL = "redacted";
    private static final String ROLE_A = "role_a";
    private static final String ROLE_B = "role_b";
    private static final List<String> ALL_ROLES = List.of(USER, ROLE_A, ROLE_B);
    private static final List<IResource> ALL_RESOURCES = List.of(
        DataResource.root(), DataResource.keyspace(KS), DataResource.allTables(KS),
        DataResource.table(KS, TBL), DataResource.table(KS, ISOLATED_TBL),
        RoleResource.root(), RoleResource.role(ROLE_A), RoleResource.role(ROLE_B), RoleResource.role(USER)
    );

    @BeforeClass
    public static void setUpAuth()
    {
        ServerTestUtils.daemonInitialization();
        DatabaseDescriptor.setPermissionsValidity(0);
        DatabaseDescriptor.setDynamicDataMaskingEnabled(true);
        DatabaseDescriptor.setRolesValidity(0);
        ClusterMetadataTestHelper.reconfigureCms(ReplicationParams.ntsMeta(Collections.singletonMap(DatabaseDescriptor.getLocalDataCenter(), 1)));
        ServerTestUtils.markCMS();
        requireAuthentication();
        requireNetwork();
        CassandraDaemon.getInstanceForTesting().setupVirtualKeyspaces();
    }

    @Before
    public void setupSchemaAndRoles()
    {
        useSuperUser();
        resetGrants();
        executeNet("CREATE KEYSPACE IF NOT EXISTS " + KS + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        executeNet("CREATE TABLE IF NOT EXISTS " + KS + '.' + TBL + " (id int PRIMARY KEY, val text MASKED WITH mask_replace('" + MASKED_VAL + "'))");
        executeNet("INSERT INTO " + KS + '.' + TBL + " (id, val) VALUES (1, '" + CLEAR_VAL + "')");
        executeNet("CREATE ROLE IF NOT EXISTS " + ROLE_A + " WITH LOGIN = false");
        executeNet("CREATE ROLE IF NOT EXISTS " + ROLE_B + " WITH LOGIN = false");
        executeNet("CREATE TABLE IF NOT EXISTS " + KS + '.' + ISOLATED_TBL + " (id int PRIMARY KEY, val text)");
        executeNet("CREATE ROLE IF NOT EXISTS " + USER + " WITH LOGIN = true AND PASSWORD = '" + PASSWORD + '\'');
    }

    @Test
    public void test()
    {
        stateful().withExamples(400).withSteps(400)
                  .check(commands(() -> State::new)
                         .add(this::grantRole)
                         .add(this::revokeRole)
                         .add(this::grantDataPermission)
                         .add(this::revokeDataPermission)
                         .add(this::grantRolePermission)
                         .add(this::revokeRolePermission)
                         .add(this::grantAllPermissions)
                         .add(this::revokeAllPermissions)
                         .add(this::verifyRoleInheritance)
                         .add(this::verifyEffectivePerms)
                         .add(this::verifyDataMaskingQuery)
                         .destroyState(State::close)
                         .commandsTransformer(LoggingCommand.factory())
                         .onSuccess(onSuccess(logger))
                         .build());
    }

    private static final Gen<Gen<String>> ROLES_MIXED = Gens.mixedDistribution(ALL_ROLES);
    private static final Gen<Gen<IResource>> DATA_RESOURCES_MIXED = Gens.mixedDistribution(
        DataResource.root(), DataResource.keyspace(KS), DataResource.allTables(KS), DataResource.table(KS, TBL)
    );
    private static final Gen<Gen<Permission>> DATA_PERMS_MIXED = Gens.mixedDistribution(
        Permission.SELECT, Permission.MODIFY, Permission.ALTER, Permission.AUTHORIZE,
        Permission.DROP, Permission.UNMASK, Permission.SELECT_MASKED
    );

    private static final Gen<Gen<IResource>> ROLE_RESOURCE_MIXED = Gens.mixedDistribution(
        RoleResource.root(), RoleResource.role(ROLE_A), RoleResource.role(ROLE_B)
    );
    private static final Gen<Gen<Permission>> ROLE_PERMS_MIXED = Gens.mixedDistribution(
        Permission.ALTER, Permission.DROP, Permission.AUTHORIZE, Permission.DESCRIBE
    );

    /**
     * Execution context for each stateful walk iteration, tracking the in-memory
     * oracle model and ensuring clean grant teardown on completion.
     */
    private class State implements AutoCloseable
    {
        final AuthModel model = new AuthModel();
        final Gen<String> rolesGen;
        final Gen<IResource> dataResourcesGen;
        final Gen<Permission> dataPermsGen;
        final Gen<IResource> roleResourcesGen;
        final Gen<Permission> rolePermsGen;

        State(RandomSource rs)
        {
            resetGrants();
            this.rolesGen = ROLES_MIXED.next(rs);
            this.dataResourcesGen = DATA_RESOURCES_MIXED.next(rs);
            this.dataPermsGen = DATA_PERMS_MIXED.next(rs);
            this.roleResourcesGen = ROLE_RESOURCE_MIXED.next(rs);
            this.rolePermsGen = ROLE_PERMS_MIXED.next(rs);
        }

        @Override
        public void close()
        {
            try
            {
                verifyAll(model);
            }
            finally
            {
                resetGrants();
            }
        }
    }

    private Property.Command<State, Void, ?> grantRole(RandomSource rs, State state)
    {
        String role = state.rolesGen.next(rs);
        String grantee = state.rolesGen.next(rs);
        boolean legal = state.model.roles.isLegalGrant(grantee, role);
        String cql = String.format("GRANT %s TO %s;", role, grantee);

        return new Property.SimpleCommand<>(cql + (legal ? "" : " (expect rejection)"), s -> {
            useSuperUser();
            if(legal)
            {
                executeNet(cql);
                s.model.roles.applyGrant(grantee, role);
            }
            else
            {
                assertRejected(cql);
            }
        });
    }

    private Property.Command<State, Void, ?> revokeRole(RandomSource rs, State state)
    {
        String role = state.rolesGen.next(rs);
        String grantee = state.rolesGen.next(rs);
        boolean legal = state.model.roles.isLegalRevoke(grantee, role);
        String cql = String.format("REVOKE %s FROM %s;", role, grantee);

        return new Property.SimpleCommand<>(cql + (legal ? "" : " (expect rejection)"), s -> {
            useSuperUser();
            if(legal)
            {
                executeNet(cql);
                s.model.roles.applyRevoke(grantee, role);
            }
            else
            {
                assertRejected(cql);
            }
        });
    }

    private Property.Command<State, Void, ?> grantDataPermission(RandomSource rs, State state)
    {
        Permission perm = state.dataPermsGen.next(rs);
        IResource resource = state.dataResourcesGen.next(rs);
        String grantee = state.rolesGen.next(rs);
        String cql = String.format("GRANT %s ON %s TO %s;", perm.name(), formatTarget(resource), grantee);

        return new Property.SimpleCommand<>(cql, s  -> {
            useSuperUser();
            executeNet(cql);
            s.model.permissions.get(grantee).grant(perm, resource);
        });
    }

    private Property.Command<State, Void, ?> revokeDataPermission(RandomSource rs, State state)
    {
        Permission perm = state.dataPermsGen.next(rs);
        IResource resource = state.dataResourcesGen.next(rs);
        String grantee = state.rolesGen.next(rs);
        String cql = String.format("REVOKE %s ON %s FROM %s;", perm.name(), formatTarget(resource), grantee);

        return new Property.SimpleCommand<>(cql, s  -> {
            useSuperUser();
            executeNet(cql);
            s.model.permissions.get(grantee).revoke(perm, resource);
        });
    }

    private Property.Command<State, Void, ?> grantRolePermission(RandomSource rs, State state)
    {
        Permission perm = state.rolePermsGen.next(rs);
        IResource resource = state.roleResourcesGen.next(rs);
        String grantee = state.rolesGen.next(rs);
        String cql = String.format("GRANT %s ON %s TO %s;", perm.name(), formatTarget(resource), grantee);

        return new Property.SimpleCommand<>(cql, s  -> {
            useSuperUser();
            executeNet(cql);
            s.model.permissions.get(grantee).grant(perm, resource);
        });
    }

    private Property.Command<State, Void, ?> revokeRolePermission(RandomSource rs, State state)
    {
        Permission perm = state.rolePermsGen.next(rs);
        IResource resource = state.roleResourcesGen.next(rs);
        String grantee = state.rolesGen.next(rs);
        String cql = String.format("REVOKE %s ON %s FROM %s;", perm.name(), formatTarget(resource), grantee);

        return new Property.SimpleCommand<>(cql, s  -> {
            useSuperUser();
            executeNet(cql);
            s.model.permissions.get(grantee).revoke(perm, resource);
        });
    }

    private Property.Command<State, Void, ?> grantAllPermissions(RandomSource rs, State state)
    {
        IResource resource = rs.nextBoolean() ? state.dataResourcesGen.next(rs) : state.roleResourcesGen.next(rs);
        String grantee = state.rolesGen.next(rs);
        String cql = String.format("GRANT ALL PERMISSIONS ON %s TO %s;", formatTarget(resource), grantee);

        return new Property.SimpleCommand<>(cql, s  -> {
            useSuperUser();
            executeNet(cql);
            s.model.permissions.get(grantee).grantAll(resource);
        });
    }

    private Property.Command<State, Void, ?> revokeAllPermissions(RandomSource rs, State state)
    {
        IResource resource = rs.nextBoolean() ? state.dataResourcesGen.next(rs) : state.roleResourcesGen.next(rs);
        String grantee = state.rolesGen.next(rs);
        String cql =  String.format("REVOKE ALL PERMISSIONS ON %s FROM %s;", formatTarget(resource), grantee);

        return new Property.SimpleCommand<>(cql, s  -> {
            useSuperUser();
            executeNet(cql);
            s.model.permissions.get(grantee).revokeAll(resource);
        });
    }

    private Property.Command<State, Void, ?> verifyRoleInheritance(RandomSource rs, State state)
    {
        String roleName  = state.rolesGen.next(rs);
        return new Property.SimpleCommand<>("Query inherited roles for " + roleName, s  -> {
            Set<RoleResource> expected = s.model.roles.expectedInheritedRoles(roleName);
            Set<RoleResource> actual = DatabaseDescriptor.getRoleManager().getRoles(RoleResource.role(roleName), true);
            assertEquals("Role Mismatch for " + roleName, expected, actual);
        });
    }

    private Property.Command<State, Void, ?> verifyEffectivePerms(RandomSource rs, State state)
    {
        String roleName = state.rolesGen.next(rs);
        IResource resource = ALL_RESOURCES.get(rs.nextInt(ALL_RESOURCES.size()));

        return new Property.SimpleCommand<>("Query effective permissions for " + roleName + " on " + formatTarget(resource), s  -> {
           AuthenticatedUser auth = new AuthenticatedUser(roleName);
           assertPermissions(String.format("Permission mismatch for %s on %s", roleName, resource), s.model, auth, resource);
        });
    }

    private Property.Command<State, Void, ?> verifyDataMaskingQuery(RandomSource rs, State state)
    {
        return new Property.SimpleCommand<>("Query Data Masking Behavior for " + USER, s  -> {
            assertMaskingBehavior(s.model, new AuthenticatedUser(USER), DataResource.table(KS, TBL));
        });
    }

    private void verifyAll(AuthModel model)
    {
        for (String roleName : ALL_ROLES)
        {
            Set<RoleResource> expected = model.roles.expectedInheritedRoles(roleName);
            Set<RoleResource> actual = DatabaseDescriptor.getRoleManager()
                                                         .getRoles(RoleResource.role(roleName), true);
            assertEquals(String.format("Role mismatch for %s!", roleName), expected, actual);

            AuthenticatedUser auth = new AuthenticatedUser(roleName);
            for (IResource res : ALL_RESOURCES)
                assertPermissions(String.format("Permission mismatch for %s on %s!", roleName, res), model, auth, res);
        }
        assertMaskingBehavior(model, new AuthenticatedUser(USER), DataResource.table(KS, TBL));
    }

    private void assertPermissions(String message, AuthModel model, AuthenticatedUser user, IResource res)
    {
        assertEquals(message, model.expectedPermissions(user.getName(), res), getEffectivePermissions(user, res));
    }

    private void assertMaskingBehavior(AuthModel model, AuthenticatedUser user, DataResource targetTbl)
    {
        Set<Permission> perms = model.expectedPermissions(user.getName(), targetTbl);
        try
        {
            useUser(user.getName(), PASSWORD);

            ResultSet rs = executeNet("SELECT val FROM " + KS + '.' + TBL + " WHERE id = 1");
            if (!perms.contains(Permission.SELECT))
                fail(String.format("Expected UnauthorizedException because user %s lacks SELECT on %s", user.getName(), targetTbl));

            String actualVal = rs.one().getString("val");
            String expectedVal = perms.contains(Permission.UNMASK) ? CLEAR_VAL : MASKED_VAL;
            assertEquals("Data masking mismatch!", expectedVal, actualVal);

            boolean canFilterMasked = perms.contains(Permission.SELECT_MASKED) || perms.contains(Permission.UNMASK);
            try
            {
                ResultSet filterRs = executeNet("SELECT val FROM " + KS + '.' + TBL + " WHERE val = '" + CLEAR_VAL + "' ALLOW FILTERING");
                if (!canFilterMasked)
                    fail(String.format("Expected UnauthorizedException: user %s lacks SELECT_MASKED or UNMASK to filter on masked column", user.getName()));

                String filteredVal = filterRs.one().getString("val");
                assertEquals("Filtered masked value mismatch!", expectedVal, filteredVal);
            }
            catch (UnauthorizedException | InvalidQueryException e)
            {
                if (canFilterMasked)
                    fail("Unexpected exception filtering on masked column: " + e.getMessage());
            }
        }
        catch (UnauthorizedException e)
        {
            if (perms.contains(Permission.SELECT))
                fail(String.format("Unexpected UnauthorizedException: user %s should have SELECT on %s", user.getName(), targetTbl));
        }
        finally
        {
            useSuperUser();
        }
    }

    private Set<Permission> getEffectivePermissions(AuthenticatedUser user, IResource res)
    {
        return effectivePermissions(res, r -> DatabaseDescriptor.getAuthorizer().authorize(user, r));
    }

    private static Set<Permission> effectivePermissions(IResource res, Function<IResource, Set<Permission>> grantsOn)
    {
        Set<Permission> effective = EnumSet.noneOf(Permission.class);
        for (IResource r : Resources.chain(res))
            effective.addAll(grantsOn.apply(r));
        effective.retainAll(res.applicablePermissions());
        return effective;
    }

    private void assertRejected(String cql)
    {
        try
        {
            executeNet(cql);
            fail("Expected Cassandra to reject illegal operation: " + cql);
        }
        catch (InvalidQueryException expected)
        {
            // Expected rejection from Cassandra query engine
        }
    }

    private static String formatTarget(IResource res)
    {
        if (res instanceof DataResource)
        {
            DataResource d = (DataResource) res;
            return d.isRootLevel() ? "ALL KEYSPACES"
                                   : d.isKeyspaceLevel() ? "KEYSPACE " + d.getKeyspace()
                                                         : d.isAllTablesLevel() ? "ALL TABLES IN KEYSPACE " + d.getKeyspace()
                                                                                : "TABLE " + d.getKeyspace() + '.' + d.getTable();
        }
        else if (res instanceof RoleResource)
        {
            RoleResource r = (RoleResource) res;
            return !r.hasParent() ? "ALL ROLES" : "ROLE " + r.getRoleName();
        }
        throw new IllegalArgumentException("Unknown resource: " + res);
    }

    private void resetGrants()
    {
        useSuperUser();
        for (String role : ALL_ROLES)
            for (IResource target : ALL_RESOURCES)
                executeIgnoringInvalid("REVOKE ALL PERMISSIONS ON " + formatTarget(target) + " FROM " + role);

        for (String r1 : ALL_ROLES)
            for (String r2 : ALL_ROLES)
                if (!r1.equals(r2))
                    executeIgnoringInvalid("REVOKE " + r1 + " FROM " + r2);
    }

    private void executeIgnoringInvalid(String cql)
    {
        try
        {
            executeNet(cql);
        }
        catch (InvalidQueryException ignored)
        {
            // Ignore when revoking a non-existent grant during cleanup
        }
    }

    protected <S> Property.StatefulSuccess<S, Void> onSuccess(Logger logger)
    {
        return (state, sut, history) -> logger.info("Successful for the following:\nState {}\nHistory:\n{}", state, Property.formatList("\t\t", history));
    }

    private static class AuthModel
    {
        final RoleGraphModel roles = new RoleGraphModel();
        final Map<String, PermissionsModel> permissions = Map.of(USER, new PermissionsModel(),
                                                                 ROLE_A, new PermissionsModel(),
                                                                 ROLE_B, new PermissionsModel());

        Set<Permission> expectedPermissions(String user, IResource res)
        {
            Set<Permission> expected = EnumSet.noneOf(Permission.class);
            for (RoleResource r : roles.expectedInheritedRoles(user))
                expected.addAll(permissions.get(r.getRoleName()).expectedPermissions(res));
            return expected;
        }

        @Override
        public String toString()
        {
            return "roles=" + roles.graph + ", grants=" + permissions;
        }
    }

    private static class RoleGraphModel
    {
        private final Map<String, Set<String>> graph = new HashMap<>();

        boolean isLegalGrant(String grantee, String role)
        {
            // Reject grants that would create a circular dependency in the role graph
            return doesNotInherit(grantee, role) && doesNotInherit(role, grantee);
        }

        boolean isLegalRevoke(String grantee, String role)
        {
            // Cassandra treats revoking a role from itself (e.g. REVOKE r FROM r) as a no-op
            return role.equals(grantee)
                   || graph.getOrDefault(grantee, Collections.emptySet()).contains(role);
        }

        void applyGrant(String grantee, String role)
        {
            graph.computeIfAbsent(grantee, k -> new HashSet<>()).add(role);
        }

        void applyRevoke(String grantee, String role)
        {
            graph.computeIfAbsent(grantee, k -> new HashSet<>()).remove(role);
        }

        private boolean doesNotInherit(String role, String other)
        {
            return !expectedInheritedRoles(role).contains(RoleResource.role(other));
        }

        Set<RoleResource> expectedInheritedRoles(String user)
        {
            Set<RoleResource> reachable = new HashSet<>();
            reachable.add(RoleResource.role(user));
            Queue<String> queue = new ArrayDeque<>();
            queue.add(user);

            while (!queue.isEmpty())
            {
                String curr = queue.poll();
                for (String parent : graph.getOrDefault(curr, Collections.emptySet()))
                {
                    if (reachable.add(RoleResource.role(parent)))
                        queue.add(parent);
                }
            }
            return reachable;
        }
    }

    private static class PermissionsModel
    {
        private final Map<IResource, Set<Permission>> directGrants = new HashMap<>();

        void grant(Permission perm, IResource res)
        {
            directGrants.computeIfAbsent(res, k -> new HashSet<>()).add(perm);
        }

        void revoke(Permission perm, IResource res)
        {
            Set<Permission> perms = directGrants.get(res);
            if (perms != null)
                perms.remove(perm);
        }

        void grantAll(IResource res)
        {
            directGrants.computeIfAbsent(res, k -> new HashSet<>()).addAll(res.applicablePermissions());
        }

        void revokeAll(IResource res)
        {
            Set<Permission> perms = directGrants.get(res);
            if (perms != null)
                perms.removeAll(res.applicablePermissions());
        }

        Set<Permission> expectedPermissions(IResource res)
        {
            return effectivePermissions(res, r -> directGrants.getOrDefault(r, Collections.emptySet()));
        }

        @Override
        public String toString()
        {
            return directGrants.toString();
        }
    }
}
