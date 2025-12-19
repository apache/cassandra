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

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

import static java.lang.String.format;

/**
 * Statement for DROP DATA_SOURCE [IF EXISTS] <servicename> ON TABLE <table>
 *
 * Removes a data source from system_distributed.serviceConfigs table,
 * stopping data streaming to the associated sink.
 */
public class DropDataSourceStatement extends AuthenticationStatement
{
    private final String keyspaceName;
    private final String tableName;
    private final String serviceName;
    private final boolean ifExists;

    public DropDataSourceStatement(String keyspaceName, String tableName,
                                   String serviceName, boolean ifExists)
    {
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
        this.serviceName = serviceName;
        this.ifExists = ifExists;
    }

    public boolean checkDataSourceExists() {
        String query = "SELECT * FROM %s.%s WHERE service = ? AND type = ?";
        String formattedQuery = format(query,
                                       org.apache.cassandra.schema.SchemaConstants.DISTRIBUTED_KEYSPACE_NAME,
                                       "service_configs");

        org.apache.cassandra.cql3.UntypedResultSet result = org.apache.cassandra.cql3.QueryProcessor.execute(formattedQuery,
                                                                                                             org.apache.cassandra.db.ConsistencyLevel.ONE,
                                                                                                             serviceName,
                                                                                                             "DATA_SOURCE");
        return !result.isEmpty();
    }

    public void dropDataSource() throws org.apache.cassandra.exceptions.RequestExecutionException {
        String query = "DELETE FROM %s.%s WHERE service = ? AND type = ?";
        String formattedQuery = format(query,
                                       org.apache.cassandra.schema.SchemaConstants.DISTRIBUTED_KEYSPACE_NAME,
                                       "service_configs");

        org.apache.cassandra.cql3.QueryProcessor.execute(formattedQuery,
                                                         org.apache.cassandra.db.ConsistencyLevel.ONE,
                                                         serviceName,
                                                         "DATA_SOURCE");
    }

    @Override
    public void validate(ClientState state) throws InvalidRequestException
    {
        if (keyspaceName == null || keyspaceName.isEmpty())
            throw new InvalidRequestException("Keyspace name cannot be empty");

        if (tableName == null || tableName.isEmpty())
            throw new InvalidRequestException("Table name cannot be empty");

        if (serviceName == null || serviceName.isEmpty())
            throw new InvalidRequestException("Service name cannot be empty");
    }

    @Override
    public void authorize(ClientState client)
    {
        client.ensureIsSuperuser("Only superusers are allowed to perform DROP DATA_SOURCE queries");
    }

    @Override
    public ResultMessage execute(ClientState state)
    {
        try {
            if (ifExists) {
                if (checkDataSourceExists()) {
                    dropDataSource();
                }
                // If IF EXISTS and doesn't exist, succeed silently
            } else {
                if (!checkDataSourceExists()) {
                    throw new InvalidRequestException(
                        String.format("Data source '%s' on table '%s.%s' does not exist",
                                      serviceName, keyspaceName, tableName)
                    );
                }
                dropDataSource();
            }

            return new ResultMessage.Void();
        } catch (InvalidRequestException e) {
            throw e;
        } catch (Exception e) {
            throw new InvalidRequestException("Failed to drop data source: " + e.getMessage());
        }
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.DROP_TRIGGER, keyspaceName, serviceName); // TODO: Add DROP_DATA_SOURCE audit type
    }

    @Override
    public String toString()
    {
        return format("%s (%s.%s, %s)", getClass().getSimpleName(),
                           keyspaceName, tableName, serviceName);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final ColumnIdentifier serviceName;
        private final QualifiedName tableName;
        private final boolean ifExists;

        public Raw(ColumnIdentifier serviceName, QualifiedName tableName, boolean ifExists)
        {
            this.serviceName = serviceName;
            this.tableName = tableName;
            this.ifExists = ifExists;
        }

        @Override
        public DropDataSourceStatement prepare(ClientState state)
        {
            String keyspaceName = tableName.hasKeyspace() ? tableName.getKeyspace() : state.getKeyspace();
            return new DropDataSourceStatement(keyspaceName, tableName.getName(),
                                              serviceName.toString(), ifExists);
        }
    }
}