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
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.ViewDefinition;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.transport.Event;

public class DropViewStatement extends SchemaAlteringStatement
{
    public final boolean ifExists;

    public DropViewStatement(CFName cf, boolean ifExists)
    {
        super(cf);
        this.ifExists = ifExists;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        CFMetaData baseTable = View.findBaseTable(keyspace(), columnFamily());
        if (baseTable != null)
            state.hasColumnFamilyAccess(keyspace(), baseTable.cfName, Permission.ALTER);
    }

    public void validate(ClientState state)
    {
        // validated in findIndexedCf()
    }

    public Event.SchemaChange announceMigration(boolean isLocalOnly) throws InvalidRequestException, ConfigurationException
    {
        try
        {
//            ViewDefinition view = Schema.instance.getViewDefinition(keyspace(), columnFamily());
//            if (view == null)
//            {
//                if (Schema.instance.getCFMetaData(keyspace(), columnFamily()) != null)
//                    throw new ConfigurationException(String.format("Cannot drop table '%s' in keyspace '%s'.", columnFamily(), keyspace()));
//
//                throw new ConfigurationException(String.format("Cannot drop non existing materialized view '%s' in keyspace '%s'.", columnFamily(), keyspace()));
//            }
//
//            CFMetaData baseCfm = Schema.instance.getCFMetaData(view.baseTableId);
//            if (baseCfm == null)
//            {
//                if (ifExists)
//                    throw new ConfigurationException(String.format("Cannot drop materialized view '%s' in keyspace '%s' without base CF.", columnFamily(), keyspace()));
//                else
//                    throw new InvalidRequestException(String.format("View '%s' could not be found in any of the tables of keyspace '%s'", cfName, keyspace()));
//            }

            MigrationManager.announceViewDrop(keyspace(), columnFamily(), isLocalOnly);
            return new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily());
        }
        catch (ConfigurationException e)
        {
            if (ifExists)
                return null;
            throw e;
        }
    }
}
