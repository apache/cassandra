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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.db.virtual.VirtualSchemaKeyspace;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Datacenters;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MD5Digest;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_QUERY_HANDLER_CLASS;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * State related to a client connection.
 */
public class ClientState
{
    private static final Logger logger = LoggerFactory.getLogger(ClientState.class);

    private static final Set<IResource> READABLE_SYSTEM_RESOURCES = new HashSet<>();
    private static final Set<IResource> PROTECTED_AUTH_RESOURCES = new HashSet<>();

    static
    {
        // We want these system cfs to be always readable to authenticated users since many tools rely on them
        // (nodetool, cqlsh, bulkloader, etc.)
        for (String cf : Arrays.asList(SystemKeyspace.LOCAL, SystemKeyspace.LEGACY_PEERS, SystemKeyspace.PEERS_V2))
            READABLE_SYSTEM_RESOURCES.add(DataResource.table(SchemaConstants.SYSTEM_KEYSPACE_NAME, cf));

        // make all schema tables readable by default (required by the drivers)
        SchemaKeyspaceTables.ALL.forEach(table -> READABLE_SYSTEM_RESOURCES.add(DataResource.table(SchemaConstants.SCHEMA_KEYSPACE_NAME, table)));

        // make all virtual schema tables readable by default as well
        VirtualSchemaKeyspace.instance.tables().forEach(t -> READABLE_SYSTEM_RESOURCES.add(t.metadata().resource));

        // neither clients nor tools need authentication/authorization
        if (DatabaseDescriptor.isDaemonInitialized())
        {
            PROTECTED_AUTH_RESOURCES.addAll(DatabaseDescriptor.getAuthenticator().protectedResources());
            PROTECTED_AUTH_RESOURCES.addAll(DatabaseDescriptor.getAuthorizer().protectedResources());
            PROTECTED_AUTH_RESOURCES.addAll(DatabaseDescriptor.getRoleManager().protectedResources());
        }
    }

    // Current user for the session
    private volatile AuthenticatedUser user;
    private volatile String keyspace;
    private volatile boolean issuedPreparedStatementsUseWarning;

    private static final QueryHandler cqlQueryHandler;
    static
    {
        QueryHandler handler = QueryProcessor.instance;
        String customHandlerClass = CUSTOM_QUERY_HANDLER_CLASS.getString();
        if (customHandlerClass != null)
        {
            try
            {
                handler = FBUtilities.construct(customHandlerClass, "QueryHandler");
                logger.info("Using {} as a query handler for native protocol queries (as requested by the {} system property)",
                            customHandlerClass, CUSTOM_QUERY_HANDLER_CLASS.getKey());
            }
            catch (Exception e)
            {
                logger.error("Cannot use class {} as query handler", customHandlerClass, e);
                JVMStabilityInspector.killCurrentJVM(e, true);
            }
        }
        cqlQueryHandler = handler;
    }

    // isInternal is used to mark ClientState as used by some internal component
    // that should have an ability to modify system keyspace.
    public final boolean isInternal;

    // The remote address of the client - null for internal clients.
    private final InetSocketAddress remoteAddress;

    // Driver String for the client
    private volatile String driverName;
    private volatile String driverVersion;
    
    // Options provided by the client
    private volatile Map<String,String> clientOptions;

    // The biggest timestamp that was returned by getTimestamp/assigned to a query. This is global to ensure that the
    // timestamp assigned are strictly monotonic on a node, which is likely what user expect intuitively (more likely,
    // most new user will intuitively expect timestamp to be strictly monotonic cluster-wise, but while that last part
    // is unrealistic expectation, doing it node-wise is easy).
    private static final AtomicLong lastTimestampMicros = new AtomicLong(0);

    @VisibleForTesting
    public static void resetLastTimestamp(long nowMillis)
    {
        long nowMicros = TimeUnit.MILLISECONDS.toMicros(nowMillis);
        if (lastTimestampMicros.get() > nowMicros)
            lastTimestampMicros.set(nowMicros);
    }

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

    protected ClientState(ClientState source)
    {
        this.isInternal = source.isInternal;
        this.remoteAddress = source.remoteAddress;
        this.user = source.user;
        this.keyspace = source.keyspace;
        this.driverName = source.driverName;
        this.driverVersion = source.driverVersion;
        this.clientOptions = source.clientOptions;
    }

    /**
     * @return a ClientState object for internal C* calls (not limited by any kind of auth).
     */
    public static ClientState forInternalCalls()
    {
        return new ClientState();
    }

    public static ClientState forInternalCalls(String keyspace)
    {
        ClientState state = new ClientState();
        state.setKeyspace(keyspace);
        return state;
    }

    /**
     * @return a ClientState object for external clients (native protocol users).
     */
    public static ClientState forExternalCalls(SocketAddress remoteAddress)
    {
        return new ClientState((InetSocketAddress)remoteAddress);
    }

    /**
     * Clone this ClientState object, but use the provided keyspace instead of the
     * keyspace in this ClientState object.
     *
     * @return a new ClientState object if the keyspace argument is non-null. Otherwise do not clone
     *   and return this ClientState object.
     */
    public ClientState cloneWithKeyspaceIfSet(String keyspace)
    {
        if (keyspace == null)
            return this;
        ClientState clientState = new ClientState(this);
        clientState.setKeyspace(keyspace);
        return clientState;
    }

    /**
     * This clock guarantees that updates for the same ClientState will be ordered
     * in the sequence seen, even if multiple updates happen in the same millisecond.
     */
    public static long getTimestamp()
    {
        while (true)
        {
            long current = currentTimeMillis() * 1000;
            long last = lastTimestampMicros.get();
            long tstamp = last >= current ? last + 1 : current;
            if (lastTimestampMicros.compareAndSet(last, tstamp))
                return tstamp;
        }
    }

    /**
     * Returns a timestamp suitable for paxos given the timestamp of the last known commit (or in progress update).
     * <p>
     * Paxos ensures that the timestamp it uses for commits respects the serial order of those commits. It does so
     * by having each replica reject any proposal whose timestamp is not strictly greater than the last proposal it
     * accepted. So in practice, which timestamp we use for a given proposal doesn't affect correctness but it does
     * affect the chance of making progress (if we pick a timestamp lower than what has been proposed before, our
     * new proposal will just get rejected).
     * <p>
     * As during the prepared phase replica send us the last propose they accepted, a first option would be to take
     * the maximum of those last accepted proposal timestamp plus 1 (and use a default value, say 0, if it's the
     * first known proposal for the partition). This would most work (giving commits the timestamp 0, 1, 2, ...
     * in the order they are commited) up to 2 important caveats:
     *   1) it would give a very poor experience when Paxos and non-Paxos updates are mixed in the same partition,
     *      since paxos operations wouldn't be using microseconds timestamps. And while you shouldn't theoretically
     *      mix the 2 kind of operations, this would still be pretty unintuitive. And what if you started writing
     *      normal updates and realize later you should switch to Paxos to enforce a property you want?
     *   2) this wouldn't actually be safe due to the expiration set on the Paxos state table.
     * <p>
     * So instead, we initially chose to use the current time in microseconds as for normal update. Which works in
     * general but mean that clock skew creates unavailability periods for Paxos updates (either a node has his clock
     * in the past and he may no be able to get commit accepted until its clock catch up, or a node has his clock in
     * the future and then once one of its commit his accepted, other nodes ones won't be until they catch up). This
     * is ok for small clock skew (few ms) but can be pretty bad for large one.
     * <p>
     * Hence our current solution: we mix both approaches. That is, we compare the timestamp of the last known
     * accepted proposal and the local time. If the local time is greater, we use it, thus keeping paxos timestamps
     * locked to the current time in general (making mixing Paxos and non-Paxos more friendly, and behaving correctly
     * when the paxos state expire (as long as your maximum clock skew is lower than the Paxos state expiration
     * time)). Otherwise (the local time is lower than the last proposal, meaning that this last proposal was done
     * with a clock in the future compared to the local one), we use the last proposal timestamp plus 1, ensuring
     * progress.
     *
     * @param minUnixMicros the max timestamp of the last proposal accepted by replica having responded
     * to the prepare phase of the paxos round this is for. In practice, that's the minimum timestamp this method
     * may return.
     * @return a timestamp suitable for a Paxos proposal (using the reasoning described above). Note that
     * contrarily to the {@link #getTimestamp()} method, the return value is not guaranteed to be unique (nor
     * monotonic) across calls since it can return it's argument (so if the same argument is passed multiple times,
     * it may be returned multiple times). Note that we still ensure Paxos "ballot" are unique (for different
     * proposal) by (securely) randomizing the non-timestamp part of the UUID.
     */
    public static long getTimestampForPaxos(long minUnixMicros)
    {
        while (true)
        {
            long current = Math.max(currentTimeMillis() * 1000, minUnixMicros);
            long last = lastTimestampMicros.get();
            long tstamp = last >= current ? last + 1 : current;
            // Note that if we ended up picking minTimestampMicrosToUse (it was "in the future"), we don't
            // want to change the local clock, otherwise a single node in the future could corrupt the clock
            // of all nodes and for all inserts (since non-paxos inserts also use lastTimestampMicros).
            // See CASSANDRA-11991
            if (tstamp == minUnixMicros || lastTimestampMicros.compareAndSet(last, tstamp))
                return tstamp;
        }
    }

    public static long getLastTimestampMicros()
    {
        return lastTimestampMicros.get();
    }

    public Optional<String> getDriverName()
    {
        return Optional.ofNullable(driverName);
    }

    public Optional<String> getDriverVersion()
    {
        return Optional.ofNullable(driverVersion);
    }

    public Optional<Map<String,String>> getClientOptions()
    {
        return Optional.ofNullable(clientOptions);
    }

    public void setDriverName(String driverName)
    {
        this.driverName = driverName;
    }

    public void setDriverVersion(String driverVersion)
    {
        this.driverVersion = driverVersion;
    }
    
    public void setClientOptions(Map<String,String> clientOptions)
    {
        this.clientOptions = ImmutableMap.copyOf(clientOptions);
    }

    public static QueryHandler getCQLQueryHandler()
    {
        return cqlQueryHandler;
    }

    public InetSocketAddress getRemoteAddress()
    {
        return remoteAddress;
    }

    InetAddress getClientAddress()
    {
        return isInternal ? null : remoteAddress.getAddress();
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

    public void setKeyspace(String ks)
    {
        // Skip keyspace validation for non-authenticated users. Apparently, some client libraries
        // call set_keyspace() before calling login(), and we have to handle that.
        if (user != null && Schema.instance.getKeyspaceMetadata(ks) == null)
            throw new InvalidRequestException("Keyspace '" + ks + "' does not exist");
        keyspace = ks;
    }

    /**
     * Attempts to login the given user.
     */
    public void login(AuthenticatedUser user)
    {
        if (user.isAnonymous() || canLogin(user))
            this.user = user;
        else
            throw new AuthenticationException(String.format("%s is not permitted to log in", user.getName()));
    }

    private boolean canLogin(AuthenticatedUser user)
    {
        try
        {
            return user.canLogin();
        }
        catch (RequestExecutionException | RequestValidationException e)
        {
            throw new AuthenticationException("Unable to perform authentication: " + e.getMessage(), e);
        }
    }

    public void ensureAllKeyspacesPermission(Permission perm)
    {
        if (isInternal)
            return;
        validateLogin();
        ensurePermission(perm, DataResource.root());
    }

    public void ensureKeyspacePermission(String keyspace, Permission perm)
    {
        ensurePermission(keyspace, perm, DataResource.keyspace(keyspace));
    }

    public void ensureAllTablesPermission(String keyspace, Permission perm)
    {
        ensurePermission(keyspace, perm, DataResource.allTables(keyspace));
    }

    public void ensureTablePermission(String keyspace, String table, Permission perm)
    {
        ensurePermission(keyspace, perm, DataResource.table(keyspace, table));
    }

    public void ensureTablePermission(TableMetadataRef tableRef, Permission perm)
    {
        ensureTablePermission(tableRef.get(), perm);
    }

    public void ensureTablePermission(TableMetadata table, Permission perm)
    {
        ensurePermission(table.keyspace, perm, table.resource);
    }

    public boolean hasTablePermission(TableMetadata table, Permission perm)
    {
        if (isInternal)
            return true;

        validateLogin();

        if (!DatabaseDescriptor.getAuthorizer().requireAuthorization())
            return true;

        List<? extends IResource> resources = Resources.chain(table.resource);
        if (DatabaseDescriptor.getAuthFromRoot())
            resources = Lists.reverse(resources);

        for (IResource r : resources)
            if (authorize(r).contains(perm))
                return true;

        return false;
    }

    private void ensurePermission(String keyspace, Permission perm, DataResource resource)
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
        ensurePermission(perm, resource);
    }

    public void ensurePermission(Permission perm, IResource resource)
    {
        if (!DatabaseDescriptor.getAuthorizer().requireAuthorization())
            return;

        // Access to built in functions is unrestricted
        if(resource instanceof FunctionResource && resource.hasParent())
            if (((FunctionResource)resource).getKeyspace().equals(SchemaConstants.SYSTEM_KEYSPACE_NAME))
                return;

        ensurePermissionOnResourceChain(perm, resource);
    }

    // Convenience method called from authorize method of CQLStatement
    // Also avoids needlessly creating lots of FunctionResource objects
    public void ensurePermission(Permission permission, Function function)
    {
        // Save creating a FunctionResource is we don't need to
        if (!DatabaseDescriptor.getAuthorizer().requireAuthorization())
            return;

        // built in functions are always available to all
        if (function.isNative())
            return;

        ensurePermissionOnResourceChain(permission, FunctionResource.function(function.name().keyspace,
                                                                              function.name().name,
                                                                              function.argTypes()));
    }

    private void ensurePermissionOnResourceChain(Permission perm, IResource resource)
    {
        List<? extends IResource> resources = Resources.chain(resource);
        if (DatabaseDescriptor.getAuthFromRoot())
            resources = Lists.reverse(resources);

        for (IResource r : resources)
            if (authorize(r).contains(perm))
                return;

        throw new UnauthorizedException(String.format("User %s has no %s permission on %s or any of its parents",
                                                      user.getName(),
                                                      perm,
                                                      resource));
    }

    private void preventSystemKSSchemaModification(String keyspace, DataResource resource, Permission perm)
    {
        // we only care about DDL statements
        if (perm != Permission.ALTER && perm != Permission.DROP && perm != Permission.CREATE)
            return;

        // prevent ALL local system keyspace modification
        if (SchemaConstants.isLocalSystemKeyspace(keyspace))
            throw new UnauthorizedException(keyspace + " keyspace is not user-modifiable.");

        if (SchemaConstants.isReplicatedSystemKeyspace(keyspace))
        {
            // allow users with sufficient privileges to alter replication params of replicated system keyspaces
            if (perm == Permission.ALTER && resource.isKeyspaceLevel())
                return;

            // prevent all other modifications of replicated system keyspaces
            throw new UnauthorizedException(String.format("Cannot %s %s", perm, resource));
        }
    }

    public void validateLogin()
    {
        if (user == null)
        {
            throw new UnauthorizedException("You have not logged in");
        }
        else if (!user.hasLocalAccess())
        {
            throw new UnauthorizedException(String.format("You do not have access to this datacenter (%s)", Datacenters.thisDatacenter()));
        }
        else
        {
            if (remoteAddress != null && !user.hasAccessFromIp(remoteAddress))
                throw new UnauthorizedException("You do not have access from this IP " + remoteAddress.getHostString());
        }
    }

    public void ensureNotAnonymous()
    {
        validateLogin();
        if (user.isAnonymous())
            throw new UnauthorizedException("You have to be logged in and not anonymous to perform this request");
    }

    /**
     * Checks if this user is an ordinary user (not a super or system user).
     *
     * @return {@code true} if this user is an ordinary user, {@code false} otherwise.
     */
    public boolean isOrdinaryUser()
    {
        return !isSuper() && !isSystem();
    }

    /**
     * Checks if this user is a super user.
     */
    public boolean isSuper()
    {
        return !DatabaseDescriptor.getAuthenticator().requireAuthentication() || (user != null && user.isSuper());
    }

    /**
     * Checks if the user is the system user.
     *
     * @return {@code true} if this user is the system user, {@code false} otherwise.
     */
    public boolean isSystem()
    {
        return isInternal;
    }

    public void ensureIsSuperuser(String message)
    {
        if (!isSuper())
            throw new UnauthorizedException(message);
    }

    public void warnAboutUseWithPreparedStatements(MD5Digest statementId, String preparedKeyspace)
    {
        if (!issuedPreparedStatementsUseWarning)
        {
            ClientWarn.instance.warn(String.format("`USE <keyspace>` with prepared statements is considered to be an anti-pattern due to ambiguity in non-qualified table names. " +
                                                   "Please consider removing instances of `Session#setKeyspace(<keyspace>)`, `Session#execute(\"USE <keyspace>\")` and `cluster.newSession(<keyspace>)` from your code, and " +
                                                   "always use fully qualified table names (e.g. <keyspace>.<table>). " +
                                                   "Keyspace used: %s, statement keyspace: %s, statement id: %s", getRawKeyspace(), preparedKeyspace, statementId));
            issuedPreparedStatementsUseWarning = true;
        }
    }

    private static void validateKeyspace(String keyspace)
    {
        if (keyspace == null)
            throw new InvalidRequestException("You have not set a keyspace for this session");
    }

    public AuthenticatedUser getUser()
    {
        return user;
    }

    private Set<Permission> authorize(IResource resource)
    {
        return user.getPermissions(resource);
    }

}
