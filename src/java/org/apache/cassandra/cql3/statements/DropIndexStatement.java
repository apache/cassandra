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

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.cql3.IndexName;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class DropIndexStatement extends SchemaAlteringStatement
{
    public final String indexName;
    public final boolean ifExists;

    public DropIndexStatement(IndexName indexName, boolean ifExists)
    {
        super(indexName.getCfName());
        this.indexName = indexName.getIdx();
        this.ifExists = ifExists;
    }

    public String columnFamily()
    {
        TableMetadata metadata = lookupIndexedTable();
        return metadata == null ? null : metadata.name;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        TableMetadata metadata = lookupIndexedTable();
        if (metadata == null)
            return;

        state.hasColumnFamilyAccess(metadata.keyspace, metadata.name, Permission.ALTER);
    }

    public void validate(ClientState state)
    {
        // validated in lookupIndexedTable()
    }

    @Override
    public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws RequestValidationException
    {
        Event.SchemaChange ce = announceMigration(state, false);
        return ce == null ? null : new ResultMessage.SchemaChange(ce);
    }

    public Event.SchemaChange announceMigration(QueryState queryState, boolean isLocalOnly) throws InvalidRequestException, ConfigurationException
    {
        TableMetadata current = lookupIndexedTable();
        if (current == null)
            return null;

        TableMetadata updated =
            current.unbuild()
                   .indexes(current.indexes.without(indexName))
                   .build();

        MigrationManager.announceTableUpdate(updated, isLocalOnly);
        // Dropping an index is akin to updating the CF
        // Note that we shouldn't call columnFamily() at this point because the index has been dropped and the call to lookupIndexedTable()
        // in that method would now throw.
        return new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, current.keyspace, current.name);
    }

    /**
     * The table for which the index should be dropped, or null if the index doesn't exist
     *
     * @return the metadata for the table containing the dropped index, or {@code null}
     * if the index to drop cannot be found but "IF EXISTS" is set on the statement.
     *
     * @throws InvalidRequestException if the index cannot be found and "IF EXISTS" is not
     * set on the statement.
     */
    private TableMetadata lookupIndexedTable()
    {
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace());
        if (ksm == null)
            throw new KeyspaceNotDefinedException("Keyspace " + keyspace() + " does not exist");

        return ksm.findIndexedTable(indexName)
                  .orElseGet(() -> {
                      if (ifExists)
                          return null;
                      else
                          throw new InvalidRequestException(String.format("Index '%s' could not be found in any " +
                                                                          "of the tables of keyspace '%s'",
                                                                          indexName, keyspace()));
                  });
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
