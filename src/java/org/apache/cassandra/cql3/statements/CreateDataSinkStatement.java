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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

/**
 * Statement for CREATE DATA_SINK [IF NOT EXISTS] <sinkname> WITH <uri>
 *
 * Data sinks are global cluster-level resources that define external destinations
 * for data (e.g., Kafka clusters). The URI format is expected to be like:
 * kafka://kafkacluster:port?param1=value1&param2=value2
 */
public class CreateDataSinkStatement extends AuthenticationStatement
{
    private final String sinkName;
    private final String uri;
    private final boolean ifNotExists;

    public CreateDataSinkStatement(String sinkName, String uri, boolean ifNotExists)
    {
        this.sinkName = sinkName;
        this.uri = uri;
        this.ifNotExists = ifNotExists;
    }

    @Override
    public void validate(ClientState state) throws InvalidRequestException {
        if (sinkName == null || sinkName.isEmpty())
            throw new InvalidRequestException("Data sink name cannot be empty");

        if (uri == null || uri.isEmpty())
            throw new InvalidRequestException("Data sink URI cannot be empty");

        // Basic URI validation
        if (!uri.contains("://"))
            throw new InvalidRequestException("Invalid URI format for data sink. Expected format: <protocol>://<host>:<port>");

        validateUri(uri);
    }

    void validateUri(String uri) throws InvalidRequestException {
        if (!uri.contains("://")) {
            throw new InvalidRequestException(
                "Invalid URI format. Expected format: <protocol>://<host>:<port>");
        }

        try {
            URI parsedUri = new URI(uri);
            String scheme = parsedUri.getScheme();

            // Validate known protocols using DataSinkConfig
            if (scheme == null) {
                throw new InvalidRequestException("URI must specify a protocol (e.g., kafka://)");
            }

            // Check if protocol is supported
            DataSinkConfig.getProtocol(scheme);

        } catch (URISyntaxException e) {
            throw new InvalidRequestException(
            String.format("Invalid URI syntax: %s", e.getMessage()));
        }
    }

    /**
     * Parses the URI to extract configuration parameters for the data sink.
     * Uses DataSinkConfig to enforce allowlist of supported/safe params.
     * URI format: protocol://host:port?param1=value1&param2=value2
     */
    private Map<String, String> parseUriToConfig(String uri) throws InvalidRequestException {
        try {
            URI parsedUri = new URI(uri);
            String scheme = parsedUri.getScheme();
            String host = parsedUri.getHost();
            int port = parsedUri.getPort();
            String query = parsedUri.getQuery();

            // Get protocol-specific config handler
            DataSinkConfig sinkConfig = DataSinkConfig.getProtocol(scheme);

            Map<String, String> config = sinkConfig.getDefaults(host, port);

            // Parse query params to override defaults
            if (query != null && !query.isEmpty()) {
                Set<String> allowedParams = sinkConfig.getAllowedParameters();
                String[] pairs = query.split("&");

                for (String pair : pairs) {
                    String[] keyValue = pair.split("=", 2);
                    if (keyValue.length == 2) {
                        String paramName = keyValue[0];

                        // Validate against protocol-specific allowlist
                        if (!allowedParams.contains(paramName)) {
                            throw new InvalidRequestException(
                                String.format("Configuration parameter '%s' not allowed for %s data sinks. " +
                                            "Allowed parameters: %s",
                                            paramName,
                                            scheme,
                                            String.join(", ", allowedParams))
                            );
                        }

                        config.put(paramName, keyValue[1]);
                    }
                }
            }

            return config;

        } catch (URISyntaxException e) {
            throw new InvalidRequestException("Invalid URI syntax: " + e.getMessage());
        }
    }

    /**
     * Checks if a data sink with the given name already exists in the service_configs table.
     *
     * @return true if the sink exists, false otherwise
     */
    private boolean dataSinkExists()
    {
        String query = "SELECT * FROM %s.%s WHERE type = ? AND service = ?";
        String formattedQuery = String.format(query,
                                              SchemaConstants.DISTRIBUTED_KEYSPACE_NAME,
                                              "service_configs");

        org.apache.cassandra.cql3.UntypedResultSet result =
            org.apache.cassandra.cql3.QueryProcessor.execute(formattedQuery,
                                                             org.apache.cassandra.db.ConsistencyLevel.ONE,
                                                             "DATA_SINK",
                                                             sinkName);
        return !result.isEmpty();
    }

    /**
     * CREATE DATA_SINK [sink_name] WITH [uri]
     * Inserts the data sink configuration into the service_configs table.
     */
    void createDataSink() throws org.apache.cassandra.exceptions.RequestExecutionException, InvalidRequestException
    {

            String query = String.format("INSERT INTO %s.%s (type, service, config) " +
                                         "VALUES (?, ?, ?)",
                                         SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, "service_configs");

            // Parse URI to extract configuration
            Map<String, String> config = parseUriToConfig(uri);

            // Add sink metadata
            config.put("sink_name", sinkName);
            config.put("uri", uri);

            org.apache.cassandra.cql3.QueryProcessor.execute(query,
                                                             org.apache.cassandra.db.ConsistencyLevel.ONE,
                                                             "DATA_SINK",
                                                             sinkName,
                                                             config);
    }

    @Override
    public void authorize(ClientState client)
    {
        client.ensureIsSuperuser("Only superusers are allowed to perform CREATE DATA_SINK queries");
    }

    @Override
    public ResultMessage execute(ClientState state)
    {
        try {
            // Check if sink already exists
            if (dataSinkExists()) {
                if (ifNotExists) {
                    // Succeed silently when using IF NOT EXISTS
                    return new ResultMessage.Void();
                }
                else
                {
                    // Throw error when sink exists and IF NOT EXISTS was not specified
                    throw new InvalidRequestException(
                        String.format("Data sink '%s' already exists", this.sinkName)
                    );
                }
            }

            // Sink doesn't exist, create it
            createDataSink();
            return new ResultMessage.Void();
        } catch (InvalidRequestException e) {
            throw e;
        } catch (Exception e) {
            throw new InvalidRequestException("Failed to create data sink: " + e.getMessage());
        }
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.CREATE_DATA_SINK, sinkName); // TODO: Add CREATE_DATA_SINK audit type
    }

    @Override
    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), sinkName, uri);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final ColumnIdentifier sinkName;
        private final String uri;
        private final boolean ifNotExists;

        public Raw(ColumnIdentifier sinkName, String uri, boolean ifNotExists)
        {
            this.sinkName = sinkName;
            this.uri = uri;
            this.ifNotExists = ifNotExists;
        }

        @Override
        public CreateDataSinkStatement prepare(ClientState state)
        {
            // Strip quotes from URI if present (STRING_LITERAL includes quotes)
            String cleanUri = uri;
            if (cleanUri.startsWith("'") && cleanUri.endsWith("'"))
                cleanUri = cleanUri.substring(1, cleanUri.length() - 1);

            return new CreateDataSinkStatement(sinkName.toString(), cleanUri, ifNotExists);
        }
    }
}