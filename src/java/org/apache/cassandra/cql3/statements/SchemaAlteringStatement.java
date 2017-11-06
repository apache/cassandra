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
package org.apache.cassandra.cql3.statements;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * Abstract class for statements that alter the schema.
 */
public abstract class SchemaAlteringStatement extends CFStatement implements CQLStatement
{
    private final boolean isColumnFamilyLevel;

    protected SchemaAlteringStatement()
    {
        super(null);
        this.isColumnFamilyLevel = false;
    }

    protected SchemaAlteringStatement(CFName name)
    {
        super(name);
        this.isColumnFamilyLevel = true;
    }

    public int getBoundTerms()
    {
        return 0;
    }

    @Override
    public void prepareKeyspace(ClientState state) throws InvalidRequestException
    {
        if (isColumnFamilyLevel)
            super.prepareKeyspace(state);
    }

    @Override
    public Prepared prepare(ClientState clientState)
    {
        // We don't allow schema changes in no-compact mode on compact tables because it feels like unnecessary
        // complication: applying the change on the non compact version of the table might be unsafe (the table is
        // still compact in general), and applying it to the compact version in a no-compact connection feels
        // confusing/unintuitive. If user want to alter the compact version, they can simply do so in a normal
        // connection; if they want to alter the non-compact version, they should finish their transition and properly
        // DROP COMPACT STORAGE on the table before doing so.
        if (isColumnFamilyLevel && clientState.isNoCompactMode())
        {
            CFMetaData table = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
            if (table.isCompactTable())
            {
                throw invalidRequest("Cannot alter schema of compact table %s.%s from a connection in NO-COMPACT mode",
                                     table.ksName, table.cfName);
            }
            else if (table.isView())
            {
                CFMetaData baseTable = Schema.instance.getView(table.ksName, table.cfName).baseTableMetadata();
                if (baseTable.isCompactTable())
                    throw new InvalidRequestException(String.format("Cannot ALTER schema of view %s.%s on compact table %s from "
                                                                    + "a connection in NO-COMPACT mode",
                                                                    table.ksName, table.cfName,
                                                                    baseTable.ksName, baseTable.cfName));
            }
        }

        return new Prepared(this);
    }

    /**
     * Schema alteration may result in a new database object (keyspace, table, role, function) being created capable of
     * having permissions GRANTed on it. The creator of the object (the primary role assigned to the AuthenticatedUser
     * performing the operation) is automatically granted ALL applicable permissions on the object. This is a hook for
     * subclasses to override in order to perform that grant when the statement is executed.
     */
    protected void grantPermissionsToCreator(QueryState state)
    {
        // no-op by default
    }

    /**
     * Announces the migration to other nodes in the cluster.
     *
     * @return the schema change event corresponding to the execution of this statement, or {@code null} if no schema change
     * has occurred (when IF NOT EXISTS is used, for example)
     *
     * @throws RequestValidationException
     */
    protected abstract Event.SchemaChange announceMigration(QueryState queryState, boolean isLocalOnly) throws RequestValidationException;

    public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws RequestValidationException
    {
        // If an IF [NOT] EXISTS clause was used, this may not result in an actual schema change.  To avoid doing
        // extra work in the drivers to handle schema changes, we return an empty message in this case. (CASSANDRA-7600)
        Event.SchemaChange ce = announceMigration(state, false);
        if (ce == null)
            return new ResultMessage.Void();

        // when a schema alteration results in a new db object being created, we grant permissions on the new
        // object to the user performing the request if:
        // * the user is not anonymous
        // * the configured IAuthorizer supports granting of permissions (not all do, AllowAllAuthorizer doesn't and
        //   custom external implementations may not)
        AuthenticatedUser user = state.getClientState().getUser();
        if (user != null && !user.isAnonymous() && ce.change == Event.SchemaChange.Change.CREATED)
        {
            try
            {
                grantPermissionsToCreator(state);
            }
            catch (UnsupportedOperationException e)
            {
                // not a problem, grant is an optional method on IAuthorizer
            }
        }

        return new ResultMessage.SchemaChange(ce);
    }

    public ResultMessage executeInternal(QueryState state, QueryOptions options)
    {
        Event.SchemaChange ce = announceMigration(state, true);
        return ce == null ? new ResultMessage.Void() : new ResultMessage.SchemaChange(ce);
    }
}
