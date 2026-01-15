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
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

import static java.lang.String.format;

/**
 * Statement for DROP DATA_SINK [IF EXISTS] <sinkname>
 * Drops a global data sink definition from the cluster.
 */
public class DropDataSinkStatement extends AuthenticationStatement
{
    private final String sinkName;
    private final boolean ifExists;

    public DropDataSinkStatement(String sinkName, boolean ifExists)
    {
        this.sinkName = sinkName;
        this.ifExists = ifExists;
    }

    public boolean checkDataSinkExists() {
        String query = "SELECT * FROM %s.%s WHERE service = ? AND type = ?";
        String formattedQuery = format(query,
                                       org.apache.cassandra.schema.SchemaConstants.DISTRIBUTED_KEYSPACE_NAME,
                                       "service_configs");

        org.apache.cassandra.cql3.UntypedResultSet result = org.apache.cassandra.cql3.QueryProcessor.execute(formattedQuery,
                                                                                                             org.apache.cassandra.db.ConsistencyLevel.ONE,
                                                                                                             sinkName,
                                                                                                             "DATA_SINK");
        return !result.isEmpty();
    }

    public boolean checkDataSinkNotInUse() {

        try {
            String query = "SELECT service, type, config FROM %s.%s";
            String formattedQuery = format(query,
                                           SchemaConstants.DISTRIBUTED_KEYSPACE_NAME,
                                           "service_configs");

            org.apache.cassandra.cql3.UntypedResultSet result = org.apache.cassandra.cql3.QueryProcessor.execute(formattedQuery,
                                                                                                                 ConsistencyLevel.ONE);

            // Check for sinks in use by current data sources
            for (org.apache.cassandra.cql3.UntypedResultSet.Row row : result) {
                // Only check DATA_SOURCE entries
                if (!"DATA_SOURCE".equals(row.getString("type"))) {
                    continue;
                }

                java.util.Map<String, String> config = row.getMap("config", UTF8Type.instance,
                                                                  UTF8Type.instance);

                if (config != null && sinkName.equals(config.get("sink"))) {
                    System.err.println("DEBUG: Found data source '" + row.getString("service") + "' using sink '" + sinkName + "'");
                    return false; // Sink in use
                }
            }
            return true; // Sink not in use
        } catch (Exception e) {
            System.err.println("DEBUG: Exception in checkDataSinkNotInUse: " + e.getMessage());
            e.printStackTrace();
            throw new InvalidRequestException("Error checking if data sink is in use: " + e.getMessage());
        }
    }


    public void dropDataSink() {

        String query = "DELETE FROM %s.%s WHERE service = ? AND type = ?";
        String formattedQuery = format(query,
                                       SchemaConstants.DISTRIBUTED_KEYSPACE_NAME,
                                       "service_configs");

        org.apache.cassandra.cql3.QueryProcessor.execute(formattedQuery,
                                                         ConsistencyLevel.ONE,
                                                         sinkName,
                                                         "DATA_SINK");
    }

    @Override
    public void validate(ClientState state) throws InvalidRequestException
    {
        if (sinkName == null || sinkName.isEmpty())
            throw new InvalidRequestException("Data sink name cannot be empty");
    }

    @Override
    public void authorize(ClientState client)
    {
        client.ensureIsSuperuser("Only superusers are allowed to perform DROP DATA_SINK queries");
    }

    @Override
    public ResultMessage execute(ClientState state)
    {
        try {
            if (ifExists) {
                if (checkDataSinkExists()) {
                    if (!checkDataSinkNotInUse()) {
                        throw new InvalidRequestException(
                            String.format("Cannot drop data sink '%s' because it is being used by one or more data sources", sinkName)
                        );
                    }
                    dropDataSink();
                }
            } else {
                if (!checkDataSinkExists()) {
                    throw new InvalidRequestException(
                        String.format("Data sink '%s' does not exist", sinkName)
                    );
                }
                if (!checkDataSinkNotInUse()) {
                    throw new InvalidRequestException(
                        String.format("Cannot drop data sink '%s' because it is being used by one or more data sources", sinkName)
                    );
                }
                dropDataSink();
            }
        } catch (InvalidRequestException e) {
            throw e;
        } catch (Exception e) {
            throw new InvalidRequestException("Failed to drop data sink: " + e.getMessage());
        }
        return null;
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.DROP_DATA_SINK, sinkName);
    }

    @Override
    public String toString()
    {
        return String.format("%s (%s)", getClass().getSimpleName(), sinkName);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final ColumnIdentifier sinkName;
        private final boolean ifExists;

        public Raw(ColumnIdentifier sinkName, boolean ifExists)
        {
            this.sinkName = sinkName;
            this.ifExists = ifExists;
        }

        @Override
        public DropDataSinkStatement prepare(ClientState state)
        {
            return new DropDataSinkStatement(sinkName.toString(), ifExists);
        }
    }
}