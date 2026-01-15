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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implie
 * d.
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

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

/**
 * Statement for CREATE DATA_SOURCE [IF NOT EXISTS] <servicename> ON TABLE <table> WITH <sinkname>
 *
 * Data sources are stored in system_distributed.serviceConfigs table and define
 * streaming configurations for tables to external sinks. The service name (e.g., "cdc")
 * indicates which data capture service to use.
 *
 * Example storage:
 * INSERT INTO system_distributed.serviceConfigs (type, service, config)
 * VALUES ('source', 'cdc', {'job_id': 'my-job', 'datacenter': 'dc1', ...});
 */
public class CreateDataSourceStatement extends AuthenticationStatement
{
    private final String keyspaceName;
    private final String tableName;
    private final String serviceName;
    private final String sinkName;
    private final boolean ifNotExists;

    public CreateDataSourceStatement(String keyspaceName, String tableName,
                                     String serviceName, String sinkName, boolean ifNotExists)
    {
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
        this.serviceName = serviceName;
        this.sinkName = sinkName;
        this.ifNotExists = ifNotExists;
    }

    private boolean dataSourceExists() {
        String query = "SELECT * FROM %s.%s WHERE type = ? AND service = ?";
        String formattedQuery = format(query,
                                        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME,
                                       "service_configs");

        UntypedResultSet result = QueryProcessor.execute(formattedQuery,
                                                         ConsistencyLevel.ONE,
                                                         "DATA_SOURCE",
                                                         serviceName);
        return !result.isEmpty();
    }

    private boolean sinkExists() {
        String query = "SELECT * FROM %s.%s WHERE type = ? AND service = ?";
        String formattedQuery = format(query,
                                       SchemaConstants.DISTRIBUTED_KEYSPACE_NAME,
                                       "service_configs");

        UntypedResultSet result = QueryProcessor.execute(formattedQuery,
                                                         ConsistencyLevel.ONE,
                                                         "DATA_SINK",
                                                         sinkName);
        return !result.isEmpty();

    }

    /**
     * Checks services supported, performing similar functionality to configAccesor classes in sidecar
     */
    void validateService() throws InvalidRequestException {
        switch(serviceName.toLowerCase()) {
            case "cdc":
//                if (!DatabaseDescriptor.isCDCEnabled()) {
//                    throw new InvalidRequestException(
//                        "CDC service is not enabled. Set cdc_enabled=true in cassandra.yaml"
//                    );
//                }
                break;

            case "kafka":
                // No necessary checks here - kafka validation already checked upon data sink creation
                break;

            default:
                 throw new InvalidRequestException(
                    String.format("Unknown service '%s'. Valid services are cdc, kafka", serviceName));
        }
    }

    private void validateKeyspaceAndTable(ClientState state) throws InvalidRequestException {
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspaceName);

        if (ksm == null) {
            throw new InvalidRequestException(
                String.format("Keyspace with name '%s' does not exist", keyspaceName)
            );
        }

        TableMetadata tableMetadata = ksm.getTableNullable(tableName);

        if (tableMetadata == null) {
            throw new InvalidRequestException(
                String.format("Table in keyspace '%s' does not exist", tableName)
            );
        }

    }
    private void createDataSource() throws org.apache.cassandra.exceptions.RequestExecutionException
    {
        if (!sinkExists()) {
            throw new InvalidRequestException(
                String.format("Sink '%s' does not exist", sinkName)
            );
        }

        // Create data source
        String query = String.format("INSERT INTO %s.%s (type, service, config) " +
                                     "VALUES (?, ?, ?)",
                                     SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, "service_configs");

        Map<String, String> config = new HashMap<>();
        config.put("keyspace", keyspaceName);
        config.put("service", serviceName);
        config.put("sink", sinkName);

        QueryProcessor.execute(query,
                               ConsistencyLevel.ONE,
                               "DATA_SOURCE",
                               serviceName,
                               config);
    }


    @Override
    public void validate(ClientState state) throws InvalidRequestException
    {
        // Validate parameters first before checking authentication
        if (keyspaceName == null || keyspaceName.isEmpty())
            throw new InvalidRequestException("Keyspace name cannot be empty");

        if (tableName == null || tableName.isEmpty())
            throw new InvalidRequestException("Table name cannot be empty");

        if (serviceName == null || serviceName.isEmpty())
            throw new InvalidRequestException("Service name cannot be empty");

        if (sinkName == null || sinkName.isEmpty())
            throw new InvalidRequestException("Sink name cannot be empty");

        // Validates service is supported - check before authentication for fail-fast
        validateService();

        // Check authentication after basic parameter validation
//        state.ensureNotAnonymous();

        // Validate keyspace and table exist
        validateKeyspaceAndTable(state);
    }

    @Override
    public void authorize(ClientState client)
    {
        client.ensureIsSuperuser("Only superusers are allowed to perform CREATE DATA_SOURCE queries");
    }

    @Override
    public ResultMessage execute(ClientState state)
    {

        if (dataSourceExists()) {
            if (ifNotExists) {
                // succceed silently
                return null;
            } else {
                throw new InvalidRequestException(
                    String.format("Data source '%s' on table '%s.%s' already exists",
                                  serviceName,
                                  keyspaceName,
                                  tableName)
                );
            }
        }
        // Handle data source does not exist
        createDataSource();
        return null;
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.CREATE_DATA_SOURCE, keyspaceName, serviceName);
    }

    @Override
    public String toString()
    {
        return String.format("%s (%s.%s, %s, %s)", getClass().getSimpleName(),
                           keyspaceName, tableName, serviceName, sinkName);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final ColumnIdentifier serviceName;
        private final QualifiedName tableName;
        private final ColumnIdentifier sinkName;
        private final boolean ifNotExists;

        public Raw(ColumnIdentifier serviceName, QualifiedName tableName,
                   ColumnIdentifier sinkName, boolean ifNotExists)
        {
            this.serviceName = serviceName;
            this.tableName = tableName;
            this.sinkName = sinkName;
            this.ifNotExists = ifNotExists;
        }

        @Override
        public CreateDataSourceStatement prepare(ClientState state)
        {
            String keyspaceName = tableName.hasKeyspace() ? tableName.getKeyspace() : state.getKeyspace();
            return new CreateDataSourceStatement(keyspaceName, tableName.getName(),
                                                serviceName.toString(), sinkName.toString(), ifNotExists);
        }
    }
}