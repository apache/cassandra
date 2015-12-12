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
package org.apache.cassandra.service;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.CassandraVersion;

/**
 * State related to a client connection.
 */
public class ClientState
{
    private static final Logger logger = LoggerFactory.getLogger(ClientState.class);
    public static final CassandraVersion DEFAULT_CQL_VERSION = org.apache.cassandra.cql3.QueryProcessor.CQL_VERSION;

    private static final Set<IResource> READABLE_SYSTEM_RESOURCES = new HashSet<>();
    private static final Set<IResource> PROTECTED_AUTH_RESOURCES = new HashSet<>();
    private static final Set<String> ALTERABLE_SYSTEM_KEYSPACES = new HashSet<>();
    private static final Set<IResource> DROPPABLE_SYSTEM_TABLES = new HashSet<>();
    static
    {
        // We want these system cfs to be always readable to authenticated users since many tools rely on them
        // (nodetool, cqlsh, bulkloader, etc.)
        for (String cf : Arrays.asList(SystemKeyspace.LOCAL, SystemKeyspace.PEERS))
            READABLE_SYSTEM_RESOURCES.add(DataResource.table(SystemKeyspace.NAME, cf));

        SchemaKeyspace.ALL.forEach(table -> READABLE_SYSTEM_RESOURCES.add(DataResource.table(SchemaKeyspace.NAME, table)));

        if (!Config.isClientMode())
        {
            PROTECTED_AUTH_RESOURCES.addAll(DatabaseDescriptor.getAuthenticator().protectedResources());
            PROTECTED_AUTH_RESOURCES.addAll(DatabaseDescriptor.getAuthorizer().protectedResources());
            PROTECTED_AUTH_RESOURCES.addAll(DatabaseDescriptor.getRoleManager().protectedResources());
        }

        // allow users with sufficient privileges to alter KS level options on AUTH_KS and
        // TRACING_KS, and also to drop legacy tables (users, credentials, permissions) from
        // AUTH_KS
        ALTERABLE_SYSTEM_KEYSPACES.add(AuthKeyspace.NAME);
        ALTERABLE_SYSTEM_KEYSPACES.add(TraceKeyspace.NAME);
        DROPPABLE_SYSTEM_TABLES.add(DataResource.table(AuthKeyspace.NAME, PasswordAuthenticator.LEGACY_CREDENTIALS_TABLE));
        DROPPABLE_SYSTEM_TABLES.add(DataResource.table(AuthKeyspace.NAME, CassandraRoleManager.LEGACY_USERS_TABLE));
        DROPPABLE_SYSTEM_TABLES.add(DataResource.table(AuthKeyspace.NAME, CassandraAuthorizer.USER_PERMISSIONS));
    }

    // Current user for the session
    private volatile AuthenticatedUser user;
    private volatile String keyspace;

    private static final QueryHandler cqlQueryHandler;
    static
    {
        QueryHandler handler = QueryProcessor.instance;
        String customHandlerClass = System.getProperty("cassandra.custom_query_handler_class");
        if (customHandlerClass != null)
        {
            try
            {
                handler = FBUtilities.construct(customHandlerClass, "QueryHandler");
                logger.info("Using {} as query handler for native protocol queries (as requested with -Dcassandra.custom_query_handler_class)", customHandlerClass);
            }
            catch (Exception e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                logger.info("Cannot use class {} as query handler ({}), ignoring by defaulting on normal query handling", customHandlerClass, e.getMessage());
            }
        }
        cqlQueryHandler = handler;
    }

    // isInternal is used to mark ClientState as used by some internal component
    // that should have an ability to modify system keyspace.
    public final boolean isInternal;

    // The remote address of the client - null for internal clients.
    private final InetSocketAddress remoteAddress;

    // The biggest timestamp that was returned by getTimestamp/assigned to a query. This is global to the VM
    // for the sake of paxos (see #9649).
    private static final AtomicLong lastTimestampMicros = new AtomicLong(0);

    /**
     * Construct a new, empty ClientState for internal calls.
     */
    private ClientState()
    {
        this.isInternal = true;
        this.remoteAddress = null;
    }

    protected ClientState(InetSocketAddress remoteAddress)
    {
        this.isInternal = false;
        this.remoteAddress = remoteAddress;
        if (!DatabaseDescriptor.getAuthenticator().requireAuthentication())
            this.user = AuthenticatedUser.ANONYMOUS_USER;
    }

    /**
     * @return a ClientState object for internal C* calls (not limited by any kind of auth).
     */
    public static ClientState forInternalCalls()
    {
        return new ClientState();
    }

    /**
     * @return a ClientState object for external clients (thrift/native protocol users).
     */
    public static ClientState forExternalCalls(SocketAddress remoteAddress)
    {
        return new ClientState((InetSocketAddress)remoteAddress);
    }

    /**
     * This clock guarantees that updates for the same ClientState will be ordered
     * in the sequence seen, even if multiple updates happen in the same millisecond.
     */
    public long getTimestamp()
    {
        while (true)
        {
            long current = System.currentTimeMillis() * 1000;
            long last = lastTimestampMicros.get();
            long tstamp = last >= current ? last + 1 : current;
            if (lastTimestampMicros.compareAndSet(last, tstamp))
                return tstamp;
        }
    }

    /**
     * This is the same than {@link #getTimestamp()} but this guarantees that the returned timestamp
     * will not be smaller than the provided {@code minTimestampToUse}.
     */
    public long getTimestamp(long minTimestampToUse)
    {
        while (true)
        {
            long current = Math.max(System.currentTimeMillis() * 1000, minTimestampToUse);
            long last = lastTimestampMicros.get();
            long tstamp = last >= current ? last + 1 : current;
            if (lastTimestampMicros.compareAndSet(last, tstamp))
                return tstamp;
        }
    }

    public static QueryHandler getCQLQueryHandler()
    {
        return cqlQueryHandler;
    }

    public InetSocketAddress getRemoteAddress()
    {
        return remoteAddress;
    }

    public String getRawKeyspace()
    {
        return keyspace;
    }

    public String getKeyspace() throws InvalidRequestException
    {
        if (keyspace == null)
            throw new InvalidRequestException("No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename");
        return keyspace;
    }

    public void setKeyspace(String ks) throws InvalidRequestException
    {
        // Skip keyspace validation for non-authenticated users. Apparently, some client libraries
        // call set_keyspace() before calling login(), and we have to handle that.
        if (user != null && Schema.instance.getKSMetaData(ks) == null)
            throw new InvalidRequestException("Keyspace '" + ks + "' does not exist");
        keyspace = ks;
    }

    /**
     * Attempts to login the given user.
     */
    public void login(AuthenticatedUser user) throws AuthenticationException
    {
        // Login privilege is not inherited via granted roles, so just
        // verify that the role with the credentials that were actually
        // supplied has it
        if (user.isAnonymous() || DatabaseDescriptor.getRoleManager().canLogin(user.getPrimaryRole()))
            this.user = user;
        else
            throw new AuthenticationException(String.format("%s is not permitted to log in", user.getName()));
    }

    public void hasAllKeyspacesAccess(Permission perm) throws UnauthorizedException
    {
        if (isInternal)
            return;
        validateLogin();
        ensureHasPermission(perm, DataResource.root());
    }

    public void hasKeyspaceAccess(String keyspace, Permission perm) throws UnauthorizedException, InvalidRequestException
    {
        hasAccess(keyspace, perm, DataResource.keyspace(keyspace));
    }

    public void hasColumnFamilyAccess(String keyspace, String columnFamily, Permission perm)
    throws UnauthorizedException, InvalidRequestException
    {
        ThriftValidation.validateColumnFamily(keyspace, columnFamily);
        hasAccess(keyspace, perm, DataResource.table(keyspace, columnFamily));
    }

    public void hasColumnFamilyAccess(CFMetaData cfm, Permission perm)
    throws UnauthorizedException, InvalidRequestException
    {
        hasAccess(cfm.ksName, perm, cfm.resource);
    }

    private void hasAccess(String keyspace, Permission perm, DataResource resource)
    throws UnauthorizedException, InvalidRequestException
    {
        validateKeyspace(keyspace);
        if (isInternal)
            return;
        validateLogin();
        preventSystemKSSchemaModification(keyspace, resource, perm);
        if ((perm == Permission.SELECT) && READABLE_SYSTEM_RESOURCES.contains(resource))
            return;
        if (PROTECTED_AUTH_RESOURCES.contains(resource))
            if ((perm == Permission.CREATE) || (perm == Permission.ALTER) || (perm == Permission.DROP))
                throw new UnauthorizedException(String.format("%s schema is protected", resource));
        ensureHasPermission(perm, resource);
    }

    public void ensureHasPermission(Permission perm, IResource resource) throws UnauthorizedException
    {
        if (!DatabaseDescriptor.getAuthorizer().requireAuthorization())
            return;

        // Access to built in functions is unrestricted
        if(resource instanceof FunctionResource && resource.hasParent())
            if (((FunctionResource)resource).getKeyspace().equals(SystemKeyspace.NAME))
                return;

        checkPermissionOnResourceChain(perm, resource);
    }

    // Convenience method called from checkAccess method of CQLStatement
    // Also avoids needlessly creating lots of FunctionResource objects
    public void ensureHasPermission(Permission permission, Function function)
    {
        // Save creating a FunctionResource is we don't need to
        if (!DatabaseDescriptor.getAuthorizer().requireAuthorization())
            return;

        // built in functions are always available to all
        if (function.isNative())
            return;

        checkPermissionOnResourceChain(permission, FunctionResource.function(function.name().keyspace,
                                                                             function.name().name,
                                                                             function.argTypes()));
    }

    private void checkPermissionOnResourceChain(Permission perm, IResource resource)
    {
        for (IResource r : Resources.chain(resource))
            if (authorize(r).contains(perm))
                return;

        throw new UnauthorizedException(String.format("User %s has no %s permission on %s or any of its parents",
                                                      user.getName(),
                                                      perm,
                                                      resource));
    }

    private void preventSystemKSSchemaModification(String keyspace, DataResource resource, Permission perm) throws UnauthorizedException
    {
        // we only care about schema modification.
        if (!((perm == Permission.ALTER) || (perm == Permission.DROP) || (perm == Permission.CREATE)))
            return;

        // prevent system keyspace modification
        if (Schema.isSystemKeyspace(keyspace))
            throw new UnauthorizedException(keyspace + " keyspace is not user-modifiable.");

        // allow users with sufficient privileges to alter KS level options on AUTH_KS and
        // TRACING_KS, and also to drop legacy tables (users, credentials, permissions) from
        // AUTH_KS
        if (ALTERABLE_SYSTEM_KEYSPACES.contains(resource.getKeyspace().toLowerCase())
           && ((perm == Permission.ALTER && !resource.isKeyspaceLevel())
               || (perm == Permission.DROP && !DROPPABLE_SYSTEM_TABLES.contains(resource))))
        {
            throw new UnauthorizedException(String.format("Cannot %s %s", perm, resource));
        }
    }

    public void validateLogin() throws UnauthorizedException
    {
        if (user == null)
            throw new UnauthorizedException("You have not logged in");
    }

    public void ensureNotAnonymous() throws UnauthorizedException
    {
        validateLogin();
        if (user.isAnonymous())
            throw new UnauthorizedException("You have to be logged in and not anonymous to perform this request");
    }

    public void ensureIsSuper(String message) throws UnauthorizedException
    {
        if (DatabaseDescriptor.getAuthenticator().requireAuthentication() && (user == null || !user.isSuper()))
            throw new UnauthorizedException(message);
    }

    private static void validateKeyspace(String keyspace) throws InvalidRequestException
    {
        if (keyspace == null)
            throw new InvalidRequestException("You have not set a keyspace for this session");
    }

    public AuthenticatedUser getUser()
    {
        return user;
    }

    public static CassandraVersion[] getCQLSupportedVersion()
    {
        return new CassandraVersion[]{ QueryProcessor.CQL_VERSION };
    }

    private Set<Permission> authorize(IResource resource)
    {
        return user.getPermissions(resource);
    }
}
