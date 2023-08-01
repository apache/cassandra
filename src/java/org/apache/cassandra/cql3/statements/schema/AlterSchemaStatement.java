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
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.messages.ResultMessage;

abstract public class AlterSchemaStatement implements CQLStatement.SingleKeyspaceCqlStatement, SchemaTransformation
{
    protected final String keyspaceName; // name of the keyspace affected by the statement
    protected ClientState state;

    protected AlterSchemaStatement(String keyspaceName)
    {
        this.keyspaceName = keyspaceName;
    }

    public void validate(ClientState state)
    {
        // validation is performed while executing the statement, in apply()

        // Cache our ClientState for use by guardrails
        this.state = state;
    }

    public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime)
    {
        return execute(state, false);
    }

    @Override
    public String keyspace()
    {
        return keyspaceName;
    }

    public ResultMessage executeLocally(QueryState state, QueryOptions options)
    {
        return execute(state, true);
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

    public ResultMessage execute(QueryState state, boolean locally)
    {
        if (SchemaConstants.isLocalSystemKeyspace(keyspaceName))
            throw ire("System keyspace '%s' is not user-modifiable", keyspaceName);

        KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
        if (null != keyspace && keyspace.isVirtual())
            throw ire("Virtual keyspace '%s' is not user-modifiable", keyspaceName);

        validateKeyspaceName();

        SchemaTransformationResult result = Schema.instance.transform(this, locally);

        clientWarnings(result.diff).forEach(ClientWarn.instance::warn);

        if (result.diff.isEmpty())
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
            createdResources(result.diff).forEach(r -> grantPermissionsOnResource(r, user));

        return new ResultMessage.SchemaChange(schemaChangeEvent(result.diff));
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
}
