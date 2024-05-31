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
package org.apache.cassandra.cql3.statements.schema;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.messages.ResultMessage;

abstract public class AlterSchemaStatement implements CQLStatement.SingleKeyspaceCqlStatement, SchemaTransformation
{
    protected final String keyspaceName; // name of the keyspace affected by the statement
    protected ClientState state;
    // TODO: not sure if this is going to stay the same, or will be replaced by more efficient serialization/sanitation means
    // or just `toString` for every statement
    private String cql;

    protected AlterSchemaStatement(String keyspaceName)
    {
        this.keyspaceName = keyspaceName;
    }

    public void setCql(String cql)
    {
        this.cql = cql;
    }

    @Override
    public String cql()
    {
        assert cql != null;
        return cql;
    }

    @Override
    public void enterExecution()
    {
        ClientWarn.instance.pauseCapture();
        ClientState localState = state;
        if (localState != null)
            localState.pauseGuardrails();
    }

    @Override
    public void exitExecution()
    {
        ClientWarn.instance.resumeCapture();
        ClientState localState = state;
        if (localState != null)
            localState.resumeGuardrails();
    }

    // TODO: validation should be performed during application
    public void validate(ClientState state)
    {
        // validation is performed while executing the statement, in apply()

        // Cache our ClientState for use by guardrails
        this.state = state;
    }

    @Override
    public ResultMessage execute(QueryState state, QueryOptions options, Dispatcher.RequestTime requestTime)
    {
        return execute(state);
    }

    @Override
    public String keyspace()
    {
        return keyspaceName;
    }

    public ResultMessage executeLocally(QueryState state, QueryOptions options)
    {
        return execute(state);
    }

    /**
     * TODO: document
     */
    abstract SchemaChange schemaChangeEvent(KeyspacesDiff diff);

    /**
     * Schema alteration may result in a new database object (keyspace, table, role, function) being created capable of
     * having permissions GRANTed on it. The creator of the object (the primary role assigned to the AuthenticatedUser
     * performing the operation) is automatically granted ALL applicable permissions on the object. This is a hook for
     * subclasses to override in order indicate which resources to to perform that grant on when the statement is executed.
     *
     * Only called if the transformation resulted in a non-empty diff.
     */
    Set<IResource> createdResources(KeyspacesDiff diff)
    {
        return ImmutableSet.of();
    }

    /**
     * Schema alteration might produce a client warning (e.g. a warning to run full repair when increading RF of a keyspace).
     * This method should be used to generate them instead of calling warn() in transformation code.
     *
     * Only called if the transformation resulted in a non-empty diff.
     */
    Set<String> clientWarnings(KeyspacesDiff diff)
    {
        return ImmutableSet.of();
    }

    public ResultMessage execute(QueryState state)
    {
        if (SchemaConstants.isLocalSystemKeyspace(keyspaceName))
            throw ire("System keyspace '%s' is not user-modifiable", keyspaceName);

        KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
        if (null != keyspace && keyspace.isVirtual())
            throw ire("Virtual keyspace '%s' is not user-modifiable", keyspaceName);

        validateKeyspaceName();
        // Perform a 'dry-run' attempt to apply the transformation locally before submitting to the CMS. This can save a
        // round trip to the CMS for things syntax errors, but also fail fast for things like configuration errors.
        // Such failures may be dependent on the specific node's config (for things like guardrails/memtable
        // config/etc), but executing a schema change which has already been committed by the CMS should always succeed
        // or else the node cannot make progress on any subsequent metadata changes. For this reason, validation errors
        // during execution are trapped and the node will fall back to safe default config wherever possible. Attempting
        // to apply the SchemaTransformation at this point will catch any such error which occurs locally before
        // submission to the CMS, but it can't guarantee that the statement can be applied as-is on every node in the
        // cluster, as config can be heterogenous falling back to safe defaults may occur on some nodes.
        ClusterMetadata metadata = ClusterMetadata.current();
        apply(metadata);

        ClusterMetadata result = Schema.instance.submit(this);

        KeyspacesDiff diff = Keyspaces.diff(metadata.schema.getKeyspaces(), result.schema.getKeyspaces());
        clientWarnings(diff).forEach(ClientWarn.instance::warn);

        if (diff.isEmpty())
            return new ResultMessage.Void();

        /*
         * When a schema alteration results in a new db object being created, we grant permissions on the new
         * object to the user performing the request if:
         * - the user is not anonymous
         * - the configured IAuthorizer supports granting of permissions (not all do, AllowAllAuthorizer doesn't and
         *   custom external implementations may not)
         */
        AuthenticatedUser user = state.getClientState().getUser();
        if (null != user && !user.isAnonymous())
            createdResources(diff).forEach(r -> grantPermissionsOnResource(r, user));

        return new ResultMessage.SchemaChange(schemaChangeEvent(diff));
    }

    private void validateKeyspaceName()
    {
        if (!SchemaConstants.isValidName(keyspaceName))
        {
            throw ire("Keyspace name must not be empty, more than %d characters long, " +
                      "or contain non-alphanumeric-underscore characters (got '%s')",
                      SchemaConstants.NAME_LENGTH, keyspaceName);
        }
    }

    protected void validateDefaultTimeToLive(TableParams params)
    {
        if (params.defaultTimeToLive == 0
            && !SchemaConstants.isSystemKeyspace(keyspaceName)
            && TimeWindowCompactionStrategy.class.isAssignableFrom(params.compaction.klass()))
            Guardrails.zeroTTLOnTWCSEnabled.ensureEnabled(state);
    }

    private void grantPermissionsOnResource(IResource resource, AuthenticatedUser user)
    {
        try
        {
            DatabaseDescriptor.getAuthorizer()
                              .grant(AuthenticatedUser.SYSTEM_USER,
                                     resource.applicablePermissions(),
                                     resource,
                                     user.getPrimaryRole());
        }
        catch (UnsupportedOperationException e)
        {
            // not a problem - grant is an optional method on IAuthorizer
        }
    }

    static InvalidRequestException ire(String format, Object... args)
    {
        return new InvalidRequestException(String.format(format, args));
    }

    public String toString()
    {
        return "AlterSchemaStatement{" +
               "keyspaceName='" + keyspaceName + '\'' +
               ", cql='" + cql() + '\'' +
               '}';
    }
}
