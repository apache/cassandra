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

package org.apache.cassandra.cql3.functions;

import java.security.AccessControlException;
import java.security.AllPermission;
import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.Collections;
import java.util.Enumeration;

/**
 * Custom {@link SecurityManager} and {@link Policy} implementation that only performs access checks
 * if explicitly enabled.
 * <p>
 * This implementation gives no measurable performance panalty
 * (see <a href="http://cstar.datastax.com/tests/id/1d461628-12ba-11e5-918f-42010af0688f">see cstar test</a>).
 * This is better than the penalty of 1 to 3 percent using a standard {@code SecurityManager} with an <i>allow all</i> policy.
 * </p>
 */
public final class ThreadAwareSecurityManager extends SecurityManager
{
    static final PermissionCollection noPermissions = new PermissionCollection()
    {
        public void add(Permission permission)
        {
            throw new UnsupportedOperationException();
        }

        public boolean implies(Permission permission)
        {
            return false;
        }

        public Enumeration<Permission> elements()
        {
            return Collections.emptyEnumeration();
        }
    };

    private static final RuntimePermission CHECK_MEMBER_ACCESS_PERMISSION = new RuntimePermission("accessDeclaredMembers");
    private static final RuntimePermission MODIFY_THREAD_PERMISSION = new RuntimePermission("modifyThread");
    private static final RuntimePermission MODIFY_THREADGROUP_PERMISSION = new RuntimePermission("modifyThreadGroup");

    private static volatile boolean installed;

    public static void install()
    {
        if (installed)
            return;
        System.setSecurityManager(new ThreadAwareSecurityManager());
        installed = true;
    }

    static
    {
        //
        // Use own security policy to be easier (and faster) since the C* has no fine grained permissions.
        // Either code has access to everything or code has access to nothing (UDFs).
        // This also removes the burden to maintain and configure policy files for production, unit tests etc.
        //
        // Note: a permission is only granted, if there is no objector. This means that
        // AccessController/AccessControlContext collect all applicable ProtectionDomains - only if none of these
        // applicable ProtectionDomains denies access, the permission is granted.
        // A ProtectionDomain can have its origin at an oridinary code-source or provided via a
        // AccessController.doPrivileded() call.
        //
        Policy.setPolicy(new Policy()
        {
            public PermissionCollection getPermissions(CodeSource codesource)
            {
                // contract of getPermissions() methods is to return a _mutable_ PermissionCollection

                Permissions perms = new Permissions();

                if (codesource == null || codesource.getLocation() == null)
                    return perms;

                switch (codesource.getLocation().getProtocol())
                {
                    case "file":
                        // All JARs and class files reside on the file system - we can safely
                        // assume that these classes are "good".
                        perms.add(new AllPermission());
                        return perms;
                }

                return perms;
            }

            public PermissionCollection getPermissions(ProtectionDomain domain)
            {
                return getPermissions(domain.getCodeSource());
            }

            public boolean implies(ProtectionDomain domain, Permission permission)
            {
                CodeSource codesource = domain.getCodeSource();
                if (codesource == null || codesource.getLocation() == null)
                    return false;

                switch (codesource.getLocation().getProtocol())
                {
                    case "file":
                        // All JARs and class files reside on the file system - we can safely
                        // assume that these classes are "good".
                        return true;
                }

                return false;
            }
        });
    }

    private static final ThreadLocal<Boolean> initializedThread = new ThreadLocal<>();

    private ThreadAwareSecurityManager()
    {
    }

    private static boolean isSecuredThread()
    {
        ThreadGroup tg = Thread.currentThread().getThreadGroup();
        if (!(tg instanceof SecurityThreadGroup))
            return false;
        Boolean threadInitialized = initializedThread.get();
        if (threadInitialized == null)
        {
            initializedThread.set(false);
            ((SecurityThreadGroup) tg).initializeThread();
            initializedThread.set(true);
            threadInitialized = true;
        }
        return threadInitialized;
    }

    public void checkAccess(Thread t)
    {
        // need to override since the default implementation only checks the permission if the current thread's
        // in the root-thread-group

        if (isSecuredThread())
            throw new AccessControlException("access denied: " + MODIFY_THREAD_PERMISSION, MODIFY_THREAD_PERMISSION);
        super.checkAccess(t);
    }

    public void checkAccess(ThreadGroup g)
    {
        // need to override since the default implementation only checks the permission if the current thread's
        // in the root-thread-group

        if (isSecuredThread())
            throw new AccessControlException("access denied: " + MODIFY_THREADGROUP_PERMISSION, MODIFY_THREADGROUP_PERMISSION);
        super.checkAccess(g);
    }

    public void checkPermission(Permission perm)
    {
        if (!isSecuredThread())
            return;

        // required by JavaDriver 2.2.0-rc3 and 3.0.0-a2 or newer
        // code in com.datastax.driver.core.CodecUtils uses Guava stuff, which in turns requires this permission
        if (CHECK_MEMBER_ACCESS_PERMISSION.equals(perm))
            return;

        super.checkPermission(perm);
    }

    public void checkPermission(Permission perm, Object context)
    {
        if (isSecuredThread())
            super.checkPermission(perm, context);
    }

    public void checkPackageAccess(String pkg)
    {
        if (!isSecuredThread())
            return;

        if (!((SecurityThreadGroup) Thread.currentThread().getThreadGroup()).isPackageAllowed(pkg))
        {
            RuntimePermission perm = new RuntimePermission("accessClassInPackage." + pkg);
            throw new AccessControlException("access denied: " + perm, perm);
        }

        super.checkPackageAccess(pkg);
    }
}
