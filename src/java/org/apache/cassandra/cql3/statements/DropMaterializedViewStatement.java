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
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.MaterializedViewDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.transport.Event;

public class DropMaterializedViewStatement extends SchemaAlteringStatement
{
    public final boolean ifExists;

    public DropMaterializedViewStatement(CFName cf, boolean ifExists)
    {
        super(cf);
        this.ifExists = ifExists;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.DROP);
    }

    public void validate(ClientState state)
    {
        // validated in findIndexedCf()
    }

    public Event.SchemaChange changeEvent()
    {
        return new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily());
    }

    public boolean announceMigration(boolean isLocalOnly) throws InvalidRequestException, ConfigurationException
    {
        try
        {
            CFMetaData viewCfm = Schema.instance.getCFMetaData(keyspace(), columnFamily());
            if (viewCfm == null)
                throw new ConfigurationException(String.format("Cannot drop non existing materialized view '%s' in keyspace '%s'.", columnFamily(), keyspace()));
            if (!viewCfm.isMaterializedView())
                throw new ConfigurationException(String.format("Cannot drop non materialized view '%s' in keyspace '%s'", columnFamily(), keyspace()));

            CFMetaData baseCfm = findBaseCf();
            if (baseCfm == null)
                throw new ConfigurationException(String.format("Cannot drop materialized view '%s' in keyspace '%s' without base CF.", columnFamily(), keyspace()));

            CFMetaData updatedCfm = baseCfm.copy();
            updatedCfm.materializedViews(updatedCfm.getMaterializedViews().without(columnFamily()));
            MigrationManager.announceColumnFamilyUpdate(updatedCfm, false, isLocalOnly);
            MigrationManager.announceColumnFamilyDrop(keyspace(), columnFamily(), isLocalOnly);
            return true;
        }
        catch (ConfigurationException e)
        {
            if (ifExists)
                return false;
            throw e;
        }
    }

    private CFMetaData findBaseCf() throws InvalidRequestException
    {
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(keyspace());
        if (ksm == null)
            throw new KeyspaceNotDefinedException("Keyspace " + keyspace() + " does not exist");

        for (CFMetaData cfm : ksm.tables)
        {
            if (cfm.getMaterializedViews().get(columnFamily()).isPresent())
                return cfm;
        }

        if (ifExists)
            return null;
        else
            throw new InvalidRequestException("View '" + cfName + "' could not be found in any of the tables of keyspace '" + keyspace() + '\'');
    }
}
