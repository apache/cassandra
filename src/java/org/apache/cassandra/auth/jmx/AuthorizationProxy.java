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

import java.lang.reflect.*;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.util.Collections;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.security.auth.Subject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.MBeanWrapper;

/**
 * Provides a proxy interface to the platform's MBeanServer instance to perform
 * role-based authorization on method invocation.
 *
 * When used in conjunction with a suitable JMXAuthenticator, which attaches a CassandraPrincipal
 * to authenticated Subjects, this class uses the configured IAuthorizer to verify that the
 * subject has the required permissions to execute methods on the MBeanServer and the MBeans it
 * manages.
 *
 * Because an ObjectName may contain wildcards, meaning it represents a set of individual MBeans,
 * JMX resources don't fit well with the hierarchical approach modelled by other IResource
 * implementations and utilised by ClientState::ensurePermission etc. To enable grants to use
 * pattern-type ObjectNames, this class performs its own custom matching and filtering of resources
 * rather than pushing that down to the configured IAuthorizer. To that end, during authorization
 * it pulls back all permissions for the active subject, filtering them to retain only grants on
 * JMXResources. It then uses ObjectName::apply to assert whether the target MBeans are wholly
 * represented by the resources with permissions. This means that it cannot use the PermissionsCache
 * as IAuthorizer can, so it manages its own cache locally.
 *
 * Methods are split into 2 categories; those which are to be invoked on the MBeanServer itself
 * and those which apply to MBean instances. Actually, this is somewhat of a construct as in fact
 * *all* invocations are performed on the MBeanServer instance, the distinction is made here on
 * those methods which take an ObjectName as their first argument and those which do not.
 * Invoking a method of the former type, e.g. MBeanServer::getAttribute(ObjectName name, String attribute),
 * implies that the caller is concerned with a specific MBean. Conversely, invoking a method such as
 * MBeanServer::getDomains is primarily a function of the MBeanServer itself. This class makes
 * such a distinction in order to identify which JMXResource the subject requires permissions on.
 *
 * Certain operations are never allowed for users and these are recorded in a deny list so that we
 * can short circuit authorization process if one is attempted by a remote subject.
 *
 */
public class AuthorizationProxy implements InvocationHandler
{
    private static final Logger logger = LoggerFactory.getLogger(AuthorizationProxy.class);

    /*
     A list of permitted methods on the MBeanServer interface which *do not* take an ObjectName
     as their first argument. These methods can be thought of as relating to the MBeanServer itself,
     rather than to the MBeans it manages. All of the allowed methods are essentially descriptive,
     hence they require the Subject to have the DESCRIBE permission on the root JMX resource.
     */
    private static final Set<String> MBEAN_SERVER_ALLOWED_METHODS = ImmutableSet.of("getDefaultDomain",
                                                                                    "getDomains",
                                                                                    "getMBeanCount",
                                                                                    "hashCode",
                                                                                    "queryMBeans",
                                                                                    "queryNames",
                                                                                    "toString");

    /*
     A list of method names which are never permitted to be executed by a remote user,
     regardless of privileges they may be granted.
     */
    private static final Set<String> DENIED_METHODS = ImmutableSet.of("createMBean",
                                                                      "deserialize",
                                                                      "getClassLoader",
                                                                      "getClassLoaderFor",
                                                                      "instantiate",
                                                                      "registerMBean",
                                                                      "unregisterMBean");

    public static final JmxPermissionsCache jmxPermissionsCache = new JmxPermissionsCache();
    private MBeanServer mbs;

    /*
     Used to check whether the Role associated with the authenticated Subject has superuser
     status. By default, just delegates to Roles::hasSuperuserStatus, but can be overridden for testing.
     */
    protected Predicate<RoleResource> isSuperuser = Roles::hasSuperuserStatus;

    /*
     Used to retrieve the set of all permissions granted to a given role. By default, this fetches
     the permissions from the local cache, which in turn loads them from the configured IAuthorizer
     but can be overridden for testing.
     */
    protected Function<RoleResource, Set<PermissionDetails>> getPermissions = jmxPermissionsCache::get;

    /*
     Used to decide whether authorization is enabled or not, usually this depends on the configured
     IAuthorizer, but can be overridden for testing.
     */
    protected BooleanSupplier isAuthzRequired = () -> DatabaseDescriptor.getAuthorizer().requireAuthorization();

    /*
     Used to find matching MBeans when the invocation target is a pattern type ObjectName.
     Defaults to querying the MBeanServer but can be overridden for testing. See checkPattern for usage.
     */
    protected Function<ObjectName, Set<ObjectName>> queryNames = (name) -> mbs.queryNames(name, null);

    /*
     Used to determine whether auth setup has completed so we know whether the expect the IAuthorizer
     to be ready. Can be overridden for testing.
     */
    protected BooleanSupplier isAuthSetupComplete = () -> StorageService.instance.isAuthSetupComplete();

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable
    {
        String methodName = method.getName();

        if ("getMBeanServer".equals(methodName))
            throw new SecurityException("Access denied");

        // Corresponds to MBeanServer.invoke
        if (methodName.equals("invoke") && args.length == 4)
            checkVulnerableMethods(args);

        // Retrieve Subject from current AccessControlContext
        AccessControlContext acc = AccessController.getContext();
        Subject subject = Subject.getSubject(acc);

        // Allow setMBeanServer iff performed on behalf of the connector server itself
        if (("setMBeanServer").equals(methodName))
        {
            if (subject != null)
                throw new SecurityException("Access denied");

            if (args[0] == null)
                throw new IllegalArgumentException("Null MBeanServer");

            if (mbs != null)
                throw new IllegalArgumentException("MBeanServer already initialized");

            mbs = (MBeanServer) args[0];
            return null;
        }

        if (authorize(subject, methodName, args))
            return invoke(method, args);

        throw new SecurityException("Access Denied");
    }

    /**
     * Performs the actual authorization of an identified subject to execute a remote method invocation.
     * @param subject The principal making the execution request. A null value represents a local invocation
     *                from the JMX connector itself
     * @param methodName Name of the method being invoked
     * @param args Array containing invocation argument. If the first element is an ObjectName instance, for
     *             authz purposes we consider this an invocation of an MBean method, otherwise it is treated
     *             as an invocation of a method on the MBeanServer.
     */
    @VisibleForTesting
    public boolean authorize(Subject subject, String methodName, Object[] args)
    {
        logger.trace("Authorizing JMX method invocation {} for {}",
                     methodName,
                     subject == null ? "" :subject.toString().replaceAll("\\n", " "));

        if (!isAuthSetupComplete.getAsBoolean())
        {
            logger.trace("Auth setup is not complete, refusing access");
            return false;
        }

        // Permissive authorization is enabled
        if (!isAuthzRequired.getAsBoolean())
            return true;

        // Allow operations performed locally on behalf of the connector server itself
        if (subject == null)
            return true;

        // Restrict access to certain methods by any remote user
        if (DENIED_METHODS.contains(methodName))
        {
            logger.trace("Access denied to restricted method {}", methodName);
            return false;
        }

        // Reject if the user has not authenticated
        Set<Principal> principals = subject.getPrincipals();
        if (principals == null || principals.isEmpty())
            return false;

        // Currently, we assume that the first Principal returned from the Subject
        // is the one to use for authorization. It would be good to make this more
        // robust, but we have no control over which Principals a given LoginModule
        // might choose to associate with the Subject following successful authentication
        RoleResource userResource = RoleResource.role(principals.iterator().next().getName());
        // A role with superuser status can do anything
        if (isSuperuser.test(userResource))
            return true;

        // The method being invoked may be a method on an MBean, or it could belong
        // to the MBeanServer itself
        if (args != null && args[0] instanceof ObjectName)
            return authorizeMBeanMethod(userResource, methodName, args);
        else
            return authorizeMBeanServerMethod(userResource, methodName);
    }

    /**
     * Authorize execution of a method on the MBeanServer which does not take an MBean ObjectName
     * as its first argument. The allowed methods that match this criteria are generally
     * descriptive methods concerned with the MBeanServer itself, rather than with any particular
     * set of MBeans managed by the server and so we check the DESCRIBE permission on the root
     * JMXResource (representing the MBeanServer)
     *
     * @param subject
     * @param methodName
     * @return the result of the method invocation, if authorized
     * @throws Throwable
     * @throws SecurityException if authorization fails
     */
    private boolean authorizeMBeanServerMethod(RoleResource subject, String methodName)
    {
        logger.trace("JMX invocation of {} on MBeanServer requires permission {}", methodName, Permission.DESCRIBE);
        return (MBEAN_SERVER_ALLOWED_METHODS.contains(methodName) &&
                hasPermission(subject, Permission.DESCRIBE, JMXResource.root()));
    }

    /**
     * Authorize execution of a method on an MBean (or set of MBeans) which may be
     * managed by the MBeanServer. Note that this also includes the queryMBeans and queryNames
     * methods of MBeanServer as those both take an ObjectName (possibly a pattern containing
     * wildcards) as their first argument. They both of those methods also accept null arguments,
     * in which case they will be handled by authorizedMBeanServerMethod
     *
     * @param role
     * @param methodName
     * @param args
     * @return the result of the method invocation, if authorized
     * @throws Throwable
     * @throws SecurityException if authorization fails
     */
    private boolean authorizeMBeanMethod(RoleResource role, String methodName, Object[] args)
    {
        ObjectName targetBean = (ObjectName)args[0];

        // work out which permission we need to execute the method being called on the mbean
        Permission requiredPermission = getRequiredPermission(methodName);
        if (null == requiredPermission)
            return false;

        logger.trace("JMX invocation of {} on {} requires permission {}", methodName, targetBean, requiredPermission);

        // find any JMXResources upon which the authenticated subject has been granted the
        // reqired permission. We'll do ObjectName-specific filtering & matching of resources later
        Set<JMXResource> permittedResources = getPermittedResources(role, requiredPermission);

        if (permittedResources.isEmpty())
            return false;

        // finally, check the JMXResource from the grants to see if we have either
        // an exact match or a wildcard match for the target resource, whichever is
        // applicable
        return targetBean.isPattern()
                ? checkPattern(targetBean, permittedResources)
                : checkExact(targetBean, permittedResources);
    }

    /**
     * Get any grants of the required permission for the authenticated subject, regardless
     * of the resource the permission applies to as we'll do the filtering & matching in
     * the calling method
     * @param subject
     * @param required
     * @return the set of JMXResources upon which the subject has been granted the required permission
     */
    private Set<JMXResource> getPermittedResources(RoleResource subject, Permission required)
    {
        return getPermissions.apply(subject)
               .stream()
               .filter(details -> details.permission == required)
               .map(details -> (JMXResource)details.resource)
               .collect(Collectors.toSet());
    }

    /**
     * Check whether a required permission has been granted to the authenticated subject on a specific resource
     * @param subject
     * @param permission
     * @param resource
     * @return true if the Subject has been granted the required permission on the specified resource; false otherwise
     */
    private boolean hasPermission(RoleResource subject, Permission permission, JMXResource resource)
    {
        return getPermissions.apply(subject)
               .stream()
               .anyMatch(details -> details.permission == permission && details.resource.equals(resource));
    }

    /**
     * Given a set of JMXResources upon which the Subject has been granted a particular permission,
     * check whether any match the pattern-type ObjectName representing the target of the method
     * invocation. At this point, we are sure that whatever the required permission, the Subject
     * has definitely been granted it against this set of JMXResources. The job of this method is
     * only to verify that the target of the invocation is covered by the members of the set.
     *
     * @param target
     * @param permittedResources
     * @return true if all registered beans which match the target can also be matched by the
     *         JMXResources the subject has been granted permissions on; false otherwise
     */
    private boolean checkPattern(ObjectName target, Set<JMXResource> permittedResources)
    {
        // if the required permission was granted on the root JMX resource, then we're done
        if (permittedResources.contains(JMXResource.root()))
            return true;

        // Get the full set of beans which match the target pattern
        Set<ObjectName> targetNames = queryNames.apply(target);

        // Iterate over the resources the permission has been granted on. Some of these may
        // be patterns, so query the server to retrieve the full list of matching names and
        // remove those from the target set. Once the target set is empty (i.e. all required
        // matches have been satisfied), the requirement is met.
        // If there are still unsatisfied targets after all the JMXResources have been processed,
        // there are insufficient grants to permit the operation.
        for (JMXResource resource : permittedResources)
        {
            try
            {
                Set<ObjectName> matchingNames = queryNames.apply(ObjectName.getInstance(resource.getObjectName()));
                targetNames.removeAll(matchingNames);
                if (targetNames.isEmpty())
                    return true;
            }
            catch (MalformedObjectNameException e)
            {
                logger.warn("Permissions for JMX resource contains invalid ObjectName {}", resource.getObjectName());
            }
        }

        logger.trace("Subject does not have sufficient permissions on all MBeans matching the target pattern {}", target);
        return false;
    }

    /**
     * Given a set of JMXResources upon which the Subject has been granted a particular permission,
     * check whether any match the ObjectName representing the target of the method invocation.
     * At this point, we are sure that whatever the required permission, the Subject has definitely
     * been granted it against this set of JMXResources. The job of this method is only to verify
     * that the target of the invocation is matched by a member of the set.
     *
     * @param target
     * @param permittedResources
     * @return true if at least one of the permitted resources matches the target; false otherwise
     */
    private boolean checkExact(ObjectName target, Set<JMXResource> permittedResources)
    {
        // if the required permission was granted on the root JMX resource, then we're done
        if (permittedResources.contains(JMXResource.root()))
            return true;

        for (JMXResource resource : permittedResources)
        {
            try
            {
                if (ObjectName.getInstance(resource.getObjectName()).apply(target))
                    return true;
            }
            catch (MalformedObjectNameException e)
            {
                logger.warn("Permissions for JMX resource contains invalid ObjectName {}", resource.getObjectName());
            }
        }

        logger.trace("Subject does not have sufficient permissions on target MBean {}", target);
        return false;
    }

    /**
     * Mapping between method names and the permission required to invoke them. Note, these
     * names refer to methods on MBean instances invoked via the MBeanServer.
     * @param methodName
     * @return
     */
    private static Permission getRequiredPermission(String methodName)
    {
        switch (methodName)
        {
            case "getAttribute":
            case "getAttributes":
                return Permission.SELECT;
            case "setAttribute":
            case "setAttributes":
                return Permission.MODIFY;
            case "invoke":
                return Permission.EXECUTE;
            case "getInstanceOf":
            case "getMBeanInfo":
            case "hashCode":
            case "isInstanceOf":
            case "isRegistered":
            case "queryMBeans":
            case "queryNames":
                return Permission.DESCRIBE;
            default:
                logger.debug("Access denied, method name {} does not map to any defined permission", methodName);
                return null;
        }
    }

    /**
     * Invoke a method on the MBeanServer instance. This is called when authorization is not required (because
     * AllowAllAuthorizer is configured, or because the invocation is being performed by the JMXConnector
     * itself rather than by a connected client), and also when a call from an authenticated subject
     * has been successfully authorized
     *
     * @param method
     * @param args
     * @return
     * @throws Throwable
     */
    private Object invoke(Method method, Object[] args) throws Throwable
    {
        try
        {
            return method.invoke(mbs, args);
        }
        catch (InvocationTargetException e) //Catch any exception that might have been thrown by the mbeans
        {
            Throwable t = e.getCause(); //Throw the exception that nodetool etc expects
            throw t;
        }
    }

    /**
     * Query the configured IAuthorizer for the set of all permissions granted on JMXResources to a specific subject
     * @param subject
     * @return All permissions granted to the specfied subject (including those transitively inherited from
     *         any roles the subject has been granted), filtered to include only permissions granted on
     *         JMXResources
     */
    private static Set<PermissionDetails> loadPermissions(RoleResource subject)
    {
        // get all permissions for the specified subject. We'll cache them as it's likely
        // we'll receive multiple lookups for the same subject (but for different resources
        // and permissions) in quick succession
        return DatabaseDescriptor.getAuthorizer().list(AuthenticatedUser.SYSTEM_USER, Permission.ALL, null, subject)
                                                 .stream()
                                                 .filter(details -> details.resource instanceof JMXResource)
                                                 .collect(Collectors.toSet());
    }

    private void checkVulnerableMethods(Object args[])
    {
        assert args.length == 4;
        ObjectName name;
        String operationName;
        Object[] params;
        String[] signature;
        try
        {
            name = (ObjectName) args[0];
            operationName = (String) args[1];
            params = (Object[]) args[2];
            signature = (String[]) args[3];
        }
        catch (ClassCastException cce)
        {
            logger.warn("Could not interpret arguments to check vulnerable MBean invocations; did the MBeanServer interface change?", cce);
            return;
        }

        // When adding compiler directives from a file, most JDKs will log the file contents if invalid, which
        // leads to an arbitrary file read vulnerability
        checkCompilerDirectiveAddMethods(name, operationName);

        // Loading arbitrary (JVM and native) libraries from remotes
        checkJvmtiLoad(name, operationName);
        checkMLetMethods(name, operationName);
    }

    private void checkCompilerDirectiveAddMethods(ObjectName name, String operation)
    {
        if (name.getCanonicalName().equals("com.sun.management:type=DiagnosticCommand")
                && operation.equals("compilerDirectivesAdd"))
            throw new SecurityException("Access is denied!");
    }

    private void checkJvmtiLoad(ObjectName name, String operation)
    {
        if (name.getCanonicalName().equals("com.sun.management:type=DiagnosticCommand")
                && operation.equals("jvmtiAgentLoad"))
            throw new SecurityException("Access is denied!");
    }

    private void checkMLetMethods(ObjectName name, String operation)
    {
        // Inspired by MBeanServerAccessController, but that class ignores check if a SecurityManager is installed,
        // which we don't want

        if (operation == null)
            return;

        try
        {
            if (!mbs.isInstanceOf(name, "javax.management.loading.MLet"))
                return;
        }
        catch (InstanceNotFoundException infe)
        {
            return;
        }

        if (operation.equals("addURL") || operation.equals("getMBeansFromURL"))
            throw new SecurityException("Access is denied!");
    }

    public static final class JmxPermissionsCache extends AuthCache<RoleResource, Set<PermissionDetails>>
        implements JmxPermissionsCacheMBean
    {
        protected JmxPermissionsCache()
        {
            super(CACHE_NAME,
                  DatabaseDescriptor::setPermissionsValidity,
                  DatabaseDescriptor::getPermissionsValidity,
                  DatabaseDescriptor::setPermissionsUpdateInterval,
                  DatabaseDescriptor::getPermissionsUpdateInterval,
                  DatabaseDescriptor::setPermissionsCacheMaxEntries,
                  DatabaseDescriptor::getPermissionsCacheMaxEntries,
                  DatabaseDescriptor::setPermissionsCacheActiveUpdate,
                  DatabaseDescriptor::getPermissionsCacheActiveUpdate,
                  AuthorizationProxy::loadPermissions,
                  Collections::emptyMap,
                  () -> true);

            MBeanWrapper.instance.registerMBean(this, MBEAN_NAME_BASE + DEPRECATED_CACHE_NAME);
        }

        public void invalidatePermissions(String roleName)
        {
            invalidate(RoleResource.role(roleName));
        }

        @Override
        protected void unregisterMBean()
        {
            super.unregisterMBean();
            MBeanWrapper.instance.unregisterMBean(MBEAN_NAME_BASE + DEPRECATED_CACHE_NAME, MBeanWrapper.OnException.LOG);
        }
    }

    public static interface JmxPermissionsCacheMBean extends AuthCacheMBean
    {
        public static final String CACHE_NAME = "JmxPermissionsCache";
        /** @deprecated See CASSANDRA-16404 */
        @Deprecated(since = "4.1")
        public static final String DEPRECATED_CACHE_NAME = "JMXPermissionsCache";

        public void invalidatePermissions(String roleName);
    }
}
