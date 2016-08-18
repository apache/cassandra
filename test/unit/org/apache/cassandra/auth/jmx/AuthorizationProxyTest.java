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

package org.apache.cassandra.auth.jmx;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.security.auth.Subject;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AuthorizationProxyTest
{
    @BeforeClass
    public static void setup() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
    }

    JMXResource osBean = JMXResource.mbean("java.lang:type=OperatingSystem");
    JMXResource runtimeBean = JMXResource.mbean("java.lang:type=Runtime");
    JMXResource threadingBean = JMXResource.mbean("java.lang:type=Threading");
    JMXResource javaLangWildcard = JMXResource.mbean("java.lang:type=*");

    JMXResource hintsBean = JMXResource.mbean("org.apache.cassandra.hints:type=HintsService");
    JMXResource batchlogBean = JMXResource.mbean("org.apache.cassandra.db:type=BatchlogManager");
    JMXResource customBean = JMXResource.mbean("org.apache.cassandra:type=CustomBean,property=foo");
    Set<ObjectName> allBeans = objectNames(osBean, runtimeBean, threadingBean, hintsBean, batchlogBean, customBean);

    RoleResource role1 = RoleResource.role("r1");

    @Test
    public void roleHasRequiredPermission() throws Throwable
    {
        Map<RoleResource, Set<PermissionDetails>> permissions =
            ImmutableMap.of(role1, Collections.singleton(permission(role1, osBean, Permission.SELECT)));

        AuthorizationProxy proxy = new ProxyBuilder().isSuperuser((role) -> false)
                                                     .getPermissions(permissions::get)
                                                     .isAuthzRequired(() -> true)
                                                     .build();

        assertTrue(proxy.authorize(subject(role1.getRoleName()),
                                   "getAttribute",
                                   new Object[]{ objectName(osBean), "arch" }));
    }

    @Test
    public void roleDoesNotHaveRequiredPermission() throws Throwable
    {
        Map<RoleResource, Set<PermissionDetails>> permissions =
            ImmutableMap.of(role1, Collections.singleton(permission(role1, osBean, Permission.AUTHORIZE)));

        AuthorizationProxy proxy = new ProxyBuilder().isSuperuser((role) -> false)
                                                     .getPermissions(permissions::get)
                                                     .isAuthzRequired(() -> true).build();

        assertFalse(proxy.authorize(subject(role1.getRoleName()),
                                    "setAttribute",
                                    new Object[]{ objectName(osBean), "arch" }));
    }

    @Test
    public void roleHasRequiredPermissionOnRootResource() throws Throwable
    {
        Map<RoleResource, Set<PermissionDetails>> permissions =
            ImmutableMap.of(role1, Collections.singleton(permission(role1, JMXResource.root(), Permission.SELECT)));

        AuthorizationProxy proxy = new ProxyBuilder().isSuperuser((role) -> false)
                                                     .getPermissions(permissions::get)
                                                     .isAuthzRequired(() -> true)
                                                     .build();

        assertTrue(proxy.authorize(subject(role1.getRoleName()),
                                   "getAttribute",
                                   new Object[]{ objectName(osBean), "arch" }));
    }

    @Test
    public void roleHasOtherPermissionOnRootResource() throws Throwable
    {
        Map<RoleResource, Set<PermissionDetails>> permissions =
            ImmutableMap.of(role1, Collections.singleton(permission(role1, JMXResource.root(), Permission.AUTHORIZE)));

        AuthorizationProxy proxy = new ProxyBuilder().isSuperuser((role) -> false)
                                                     .getPermissions(permissions::get)
                                                     .isAuthzRequired(() -> true)
                                                     .build();

        assertFalse(proxy.authorize(subject(role1.getRoleName()),
                                    "invoke",
                                    new Object[]{ objectName(osBean), "bogusMethod" }));
    }

    @Test
    public void roleHasNoPermissions() throws Throwable
    {
        AuthorizationProxy proxy = new ProxyBuilder().isSuperuser((role) -> false)
                                                     .getPermissions((role) -> Collections.emptySet())
                                                     .isAuthzRequired(() -> true)
                                                     .build();

        assertFalse(proxy.authorize(subject(role1.getRoleName()),
                                    "getAttribute",
                                    new Object[]{ objectName(osBean), "arch" }));
    }

    @Test
    public void roleHasNoPermissionsButIsSuperuser() throws Throwable
    {
        AuthorizationProxy proxy = new ProxyBuilder().isSuperuser((role) -> true)
                                                     .getPermissions((role) -> Collections.emptySet())
                                                     .isAuthzRequired(() -> true)
                                                     .build();

        assertTrue(proxy.authorize(subject(role1.getRoleName()),
                                   "getAttribute",
                                   new Object[]{ objectName(osBean), "arch" }));
    }

    @Test
    public void roleHasNoPermissionsButAuthzNotRequired() throws Throwable
    {
        AuthorizationProxy proxy = new ProxyBuilder().isSuperuser((role) -> false)
                                                     .getPermissions((role) -> Collections.emptySet())
                                                     .isAuthzRequired(() -> false)
                                                     .build();

        assertTrue(proxy.authorize(subject(role1.getRoleName()),
                                   "getAttribute",
                                   new Object[]{ objectName(osBean), "arch" }));
    }

    @Test
    public void authorizeWhenSubjectIsNull() throws Throwable
    {
        // a null subject indicates that the action is being performed by the
        // connector itself, so we always authorize it
        // Verify that the superuser status is never tested as the request returns early
        // due to the null Subject
        // Also, hardcode the permissions provider to return an empty set, so we know that
        // can be doubly sure that it's the null Subject which causes the authz to succeed
        final AtomicBoolean suStatusChecked = new AtomicBoolean(false);
        AuthorizationProxy proxy = new ProxyBuilder().getPermissions((role) -> Collections.emptySet())
                                                     .isAuthzRequired(() -> true)
                                                     .isSuperuser((role) ->
                                                                  {
                                                                      suStatusChecked.set(true);
                                                                      return false;
                                                                  })
                                                     .build();

        assertTrue(proxy.authorize(null,
                                   "getAttribute",
                                   new Object[]{ objectName(osBean), "arch" }));
        assertFalse(suStatusChecked.get());
    }

    @Test
    public void rejectWhenSubjectNotAuthenticated() throws Throwable
    {
        // Access is denied to a Subject without any associated Principals
        // Verify that the superuser status is never tested as the request is rejected early
        // due to the Subject
        final AtomicBoolean suStatusChecked = new AtomicBoolean(false);
        AuthorizationProxy proxy = new ProxyBuilder().isAuthzRequired(() -> true)
                                                     .isSuperuser((role) ->
                                                                  {
                                                                      suStatusChecked.set(true);
                                                                      return true;
                                                                  })
                                                     .build();
        assertFalse(proxy.authorize(new Subject(),
                                    "getAttribute",
                                    new Object[]{ objectName(osBean), "arch" }));
        assertFalse(suStatusChecked.get());
    }

    @Test
    public void authorizeWhenWildcardGrantCoversExactTarget() throws Throwable
    {
        Map<RoleResource, Set<PermissionDetails>> permissions =
            ImmutableMap.of(role1, Collections.singleton(permission(role1, javaLangWildcard, Permission.SELECT)));

        AuthorizationProxy proxy = new ProxyBuilder().isAuthzRequired(() -> true)
                                                     .isSuperuser((role) -> false)
                                                     .getPermissions(permissions::get)
                                                     .build();

        assertTrue(proxy.authorize(subject(role1.getRoleName()),
                                   "getAttribute",
                                   new Object[]{ objectName(osBean), "arch" }));
    }

    @Test
    public void rejectWhenWildcardGrantDoesNotCoverExactTarget() throws Throwable
    {
        Map<RoleResource, Set<PermissionDetails>> permissions =
            ImmutableMap.of(role1, Collections.singleton(permission(role1, javaLangWildcard, Permission.SELECT)));

        AuthorizationProxy proxy = new ProxyBuilder().isAuthzRequired(() -> true)
                                                     .isSuperuser((role) -> false)
                                                     .getPermissions(permissions::get)
                                                     .build();

        assertFalse(proxy.authorize(subject(role1.getRoleName()),
                                    "getAttribute",
                                    new Object[]{ objectName(customBean), "arch" }));
    }

    @Test
    public void authorizeWhenWildcardGrantCoversWildcardTarget() throws Throwable
    {
        Map<RoleResource, Set<PermissionDetails>> permissions =
            ImmutableMap.of(role1, Collections.singleton(permission(role1, javaLangWildcard, Permission.DESCRIBE)));

        AuthorizationProxy proxy = new ProxyBuilder().isAuthzRequired(() -> true)
                                                     .isSuperuser((role) -> false)
                                                     .getPermissions(permissions::get)
                                                     .queryNames(matcher(allBeans))
                                                     .build();

        assertTrue(proxy.authorize(subject(role1.getRoleName()),
                                   "queryNames",
                                   new Object[]{ objectName(javaLangWildcard), null }));
    }

    @Test
    public void rejectWhenWildcardGrantIsDisjointWithWildcardTarget() throws Throwable
    {
        JMXResource customWildcard = JMXResource.mbean("org.apache.cassandra:*");
        Map<RoleResource, Set<PermissionDetails>> permissions =
            ImmutableMap.of(role1, Collections.singleton(permission(role1, customWildcard, Permission.DESCRIBE)));

        AuthorizationProxy proxy = new ProxyBuilder().isAuthzRequired(() -> true)
                                                     .isSuperuser((role) -> false)
                                                     .getPermissions(permissions::get)
                                                     .queryNames(matcher(allBeans))
                                                     .build();

        // the grant on org.apache.cassandra:* shouldn't permit us to invoke queryNames with java.lang:*
        assertFalse(proxy.authorize(subject(role1.getRoleName()),
                                    "queryNames",
                                    new Object[]{ objectName(javaLangWildcard), null }));
    }

    @Test
    public void rejectWhenWildcardGrantIntersectsWithWildcardTarget() throws Throwable
    {
        // in this test, permissions are granted on org.apache.cassandra:type=CustomBean,property=*
        // and all beans in the org.apache.cassandra.hints domain, but
        // but the target of the invocation is org.apache.cassandra*:*
        // i.e. the subject has permissions on all CustomBeans and on the HintsService bean, but is
        // attempting to query all names in the org.apache.cassandra* domain. The operation should
        // be rejected as the permissions don't cover all known beans matching that domain, due to
        // the BatchLogManager bean.

        JMXResource allCustomBeans = JMXResource.mbean("org.apache.cassandra:type=CustomBean,property=*");
        JMXResource allHintsBeans = JMXResource.mbean("org.apache.cassandra.hints:*");
        ObjectName allCassandraBeans = ObjectName.getInstance("org.apache.cassandra*:*");

        Map<RoleResource, Set<PermissionDetails>> permissions =
            ImmutableMap.of(role1, ImmutableSet.of(permission(role1, allCustomBeans, Permission.DESCRIBE),
                                                   permission(role1, allHintsBeans, Permission.DESCRIBE)));

        AuthorizationProxy proxy = new ProxyBuilder().isAuthzRequired(() -> true)
                                                     .isSuperuser((role) -> false)
                                                     .getPermissions(permissions::get)
                                                     .queryNames(matcher(allBeans))
                                                     .build();

        // the grant on org.apache.cassandra:* shouldn't permit us to invoke queryNames with java.lang:*
        assertFalse(proxy.authorize(subject(role1.getRoleName()),
                                    "queryNames",
                                    new Object[]{ allCassandraBeans, null }));
    }

    @Test
    public void authorizeOnTargetWildcardWithPermissionOnRoot() throws Throwable
    {
        Map<RoleResource, Set<PermissionDetails>> permissions =
            ImmutableMap.of(role1, Collections.singleton(permission(role1, JMXResource.root(), Permission.SELECT)));

        AuthorizationProxy proxy = new ProxyBuilder().isAuthzRequired(() -> true)
                                                     .isSuperuser((role) -> false)
                                                     .getPermissions(permissions::get)
                                                     .build();

        assertTrue(proxy.authorize(subject(role1.getRoleName()),
                                   "getAttribute",
                                   new Object[]{ objectName(javaLangWildcard), "arch" }));
    }

    @Test
    public void rejectInvocationOfUnknownMethod() throws Throwable
    {
        // Grant ALL permissions on the root resource, so we know that it's
        // the unknown method that causes the authz rejection. Of course, this
        // isn't foolproof but it's something.
        Set<PermissionDetails> allPerms = Permission.ALL.stream()
                                                        .map(perm -> permission(role1, JMXResource.root(), perm))
                                                        .collect(Collectors.toSet());
        Map<RoleResource, Set<PermissionDetails>> permissions = ImmutableMap.of(role1, allPerms);
        AuthorizationProxy proxy = new ProxyBuilder().isAuthzRequired(() -> true)
                                                     .isSuperuser((role) -> false)
                                                     .getPermissions(permissions::get)
                                                     .build();

        assertFalse(proxy.authorize(subject(role1.getRoleName()),
                                    "unKnownMethod",
                                    new Object[] { ObjectName.getInstance(osBean.getObjectName()) }));
    }

    @Test
    public void rejectInvocationOfBlacklistedMethods() throws Throwable
    {
        String[] methods = { "createMBean",
                             "deserialize",
                             "getClassLoader",
                             "getClassLoaderFor",
                             "instantiate",
                             "registerMBean",
                             "unregisterMBean" };

        // Hardcode the superuser status check to return true, so any allowed method can be invoked.
        AuthorizationProxy proxy = new ProxyBuilder().isAuthzRequired(() -> true)
                                                     .isSuperuser((role) -> true)
                                                     .build();

        for (String method : methods)
            // the arguments array isn't significant, so it can just be empty
            assertFalse(proxy.authorize(subject(role1.getRoleName()), method, new Object[0]));
    }

    @Test
    public void authorizeMethodsWithoutMBeanArgumentIfPermissionsGranted() throws Throwable
    {
        // Certain methods on MBeanServer don't take an ObjectName as their first argument.
        // These methods are characterised by AuthorizationProxy as being concerned with
        // the MBeanServer itself, as opposed to a specific managed bean. Of these methods,
        // only those considered "descriptive" are allowed to be invoked by remote users.
        // These require the DESCRIBE permission on the root JMXResource.
        testNonMbeanMethods(true);
    }

    @Test
    public void rejectMethodsWithoutMBeanArgumentIfPermissionsNotGranted() throws Throwable
    {
        testNonMbeanMethods(false);
    }

    @Test
    public void rejectWhenAuthSetupIsNotComplete() throws Throwable
    {
        // IAuthorizer & IRoleManager should not be considered ready to use until
        // we know that auth setup has completed. So, even though the IAuthorizer
        // would theoretically grant access, the auth proxy should deny it if setup
        // hasn't finished.

        Map<RoleResource, Set<PermissionDetails>> permissions =
        ImmutableMap.of(role1, Collections.singleton(permission(role1, osBean, Permission.SELECT)));

        // verify that access is granted when setup is complete
        AuthorizationProxy proxy = new ProxyBuilder().isSuperuser((role) -> false)
                                                     .getPermissions(permissions::get)
                                                     .isAuthzRequired(() -> true)
                                                     .isAuthSetupComplete(() -> true)
                                                     .build();

        assertTrue(proxy.authorize(subject(role1.getRoleName()),
                                   "getAttribute",
                                   new Object[]{ objectName(osBean), "arch" }));

        // and denied when it isn't
        proxy = new ProxyBuilder().isSuperuser((role) -> false)
                                  .getPermissions(permissions::get)
                                  .isAuthzRequired(() -> true)
                                  .isAuthSetupComplete(() -> false)
                                  .build();

        assertFalse(proxy.authorize(subject(role1.getRoleName()),
                                   "getAttribute",
                                   new Object[]{ objectName(osBean), "arch" }));
    }

    private void testNonMbeanMethods(boolean withPermission)
    {
        String[] methods = { "getDefaultDomain",
                             "getDomains",
                             "getMBeanCount",
                             "hashCode",
                             "queryMBeans",
                             "queryNames",
                             "toString" };


        ProxyBuilder builder = new ProxyBuilder().isAuthzRequired(() -> true).isSuperuser((role) -> false);
        if (withPermission)
        {
            Map<RoleResource, Set<PermissionDetails>> permissions =
                ImmutableMap.of(role1, ImmutableSet.of(permission(role1, JMXResource.root(), Permission.DESCRIBE)));
            builder.getPermissions(permissions::get);
        }
        else
        {
            builder.getPermissions((role) -> Collections.emptySet());
        }
        AuthorizationProxy proxy = builder.build();

        for (String method : methods)
            assertEquals(withPermission, proxy.authorize(subject(role1.getRoleName()), method, new Object[]{ null }));

        // non-whitelisted methods should be rejected regardless.
        // This isn't exactly comprehensive, but it's better than nothing
        String[] notAllowed = { "fooMethod", "barMethod", "bazMethod" };
        for (String method : notAllowed)
            assertFalse(proxy.authorize(subject(role1.getRoleName()), method, new Object[]{ null }));
    }

    // provides a simple matching function which can be substituted for the proxy's queryMBeans
    // utility (which by default just delegates to the MBeanServer)
    // This function just iterates over a supplied set of ObjectNames and filters out those
    // to which the target name *doesn't* apply
    private static Function<ObjectName, Set<ObjectName>> matcher(Set<ObjectName> allBeans)
    {
        return (target) -> allBeans.stream()
                                   .filter(target::apply)
                                   .collect(Collectors.toSet());
    }

    private static PermissionDetails permission(RoleResource grantee, IResource resource, Permission permission)
    {
        return new PermissionDetails(grantee.getRoleName(), resource, permission);
    }

    private static Subject subject(String roleName)
    {
        Subject subject = new Subject();
        subject.getPrincipals().add(new CassandraPrincipal(roleName));
        return subject;
    }

    private static ObjectName objectName(JMXResource resource) throws MalformedObjectNameException
    {
        return ObjectName.getInstance(resource.getObjectName());
    }

    private static Set<ObjectName> objectNames(JMXResource... resource)
    {
        Set<ObjectName> names = new HashSet<>();
        try
        {
            for (JMXResource r : resource)
                names.add(objectName(r));
        }
        catch (MalformedObjectNameException e)
        {
            fail("JMXResource returned invalid object name: " + e.getMessage());
        }
        return names;
    }

    public static class ProxyBuilder
    {
        Function<RoleResource, Set<PermissionDetails>> getPermissions;
        Function<ObjectName, Set<ObjectName>> queryNames;
        Function<RoleResource, Boolean> isSuperuser;
        Supplier<Boolean> isAuthzRequired;
        Supplier<Boolean> isAuthSetupComplete = () -> true;

        AuthorizationProxy build()
        {
            InjectableAuthProxy proxy = new InjectableAuthProxy();

            if (getPermissions != null)
                proxy.setGetPermissions(getPermissions);

            if (queryNames != null)
                proxy.setQueryNames(queryNames);

            if (isSuperuser != null)
                proxy.setIsSuperuser(isSuperuser);

            if (isAuthzRequired != null)
                proxy.setIsAuthzRequired(isAuthzRequired);

            proxy.setIsAuthSetupComplete(isAuthSetupComplete);

            return proxy;
        }

        ProxyBuilder getPermissions(Function<RoleResource, Set<PermissionDetails>> f)
        {
            getPermissions = f;
            return this;
        }

        ProxyBuilder queryNames(Function<ObjectName, Set<ObjectName>> f)
        {
            queryNames = f;
            return this;
        }

        ProxyBuilder isSuperuser(Function<RoleResource, Boolean> f)
        {
            isSuperuser = f;
            return this;
        }

        ProxyBuilder isAuthzRequired(Supplier<Boolean> s)
        {
            isAuthzRequired = s;
            return this;
        }

        ProxyBuilder isAuthSetupComplete(Supplier<Boolean> s)
        {
            isAuthSetupComplete = s;
            return this;
        }

        private static class InjectableAuthProxy extends AuthorizationProxy
        {
            void setGetPermissions(Function<RoleResource, Set<PermissionDetails>> f)
            {
                this.getPermissions = f;
            }

            void setQueryNames(Function<ObjectName, Set<ObjectName>> f)
            {
                this.queryNames = f;
            }

            void setIsSuperuser(Function<RoleResource, Boolean> f)
            {
                this.isSuperuser = f;
            }

            void setIsAuthzRequired(Supplier<Boolean> s)
            {
                this.isAuthzRequired = s;
            }

            void setIsAuthSetupComplete(Supplier<Boolean> s)
            {
                this.isAuthSetupComplete = s;
            }
        }
    }
}
