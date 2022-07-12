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
package org.apache.cassandra.nodes.virtual;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

import static java.lang.String.format;

/**
 * Definitions for {@code system.local}, {@code system.peers} and {@code system.peers_v2} are the same
 * as for the virtual table counterparts. The definition is outside of {@link org.apache.cassandra.db.SystemKeyspace}
 * to separate class dependencies.
 *
 * Note that we need the definitions of {@code system.local}, {@code system.peers} and {@code system.peers_v2}
 * in the schema for drivers and tools and applications.
 */
public class NodesSystemViews
{
    public static final TableMetadata LocalMetadata =
            parse(NodeConstants.LOCAL,
                  "information about the local node",
                  "CREATE TABLE %s ("
                  + "key text,"
                  + "bootstrapped text,"
                  + "broadcast_address inet,"
                  + "broadcast_port int,"
                  + "cluster_name text,"
                  + "cql_version text,"
                  + "data_center text,"
                  + "gossip_generation int,"
                  + "host_id uuid,"
                  + "listen_address inet,"
                  + "listen_port int,"
                  + "native_protocol_version text,"
                  + "partitioner text,"
                  + "rack text,"
                  + "release_version text,"
                  + "rpc_address inet,"
                  + "rpc_port int,"
                  + "schema_version uuid,"
                  + "tokens set<varchar>,"
                  + "truncated_at map<uuid, blob>,"
                  + "PRIMARY KEY ((key)))"
                  ).recordDeprecatedSystemColumn("thrift_version", UTF8Type.instance)
                   .build();

    public static final TableMetadata PeersV2Metadata =
            parse(NodeConstants.PEERS_V2,
                  "information about known peers in the cluster",
                  "CREATE TABLE %s ("
                  + "peer inet,"
                  + "peer_port int,"
                  + "data_center text,"
                  + "host_id uuid,"
                  + "preferred_ip inet,"
                  + "preferred_port int,"
                  + "rack text,"
                  + "release_version text,"
                  + "native_address inet,"
                  + "native_port int,"
                  + "schema_version uuid,"
                  + "tokens set<varchar>,"
                  + "PRIMARY KEY ((peer), peer_port))"
                  ).build();

    public static final TableMetadata LegacyPeersMetadata =
            parse(NodeConstants.LEGACY_PEERS,
                  "information about known peers in the cluster",
                  "CREATE TABLE %s ("
                  + "peer inet,"
                  + "data_center text,"
                  + "host_id uuid,"
                  + "preferred_ip inet,"
                  + "rack text,"
                  + "release_version text,"
                  + "rpc_address inet,"
                  + "schema_version uuid,"
                  + "tokens set<varchar>,"
                  + "PRIMARY KEY ((peer)))"
            ).build();

    private static TableMetadata.Builder parse(String table, String description, String cql)
    {
        return CreateTableStatement.parse(format(cql, table), SchemaConstants.SYSTEM_KEYSPACE_NAME)
                                   .id(TableId.forSystemTable(SchemaConstants.SYSTEM_KEYSPACE_NAME, table))
                                   .gcGraceSeconds(0)
                                   .memtableFlushPeriod((int) TimeUnit.HOURS.toMillis(1))
                                   .comment(description);
    }

    public static TableMetadata virtualFromLegacy(TableMetadata legacy, String table)
    {
        TableMetadata.Builder builder = TableMetadata.builder(SchemaConstants.SYSTEM_VIEWS_KEYSPACE_NAME, table)
                                                     .kind(TableMetadata.Kind.VIRTUAL);
        legacy.partitionKeyColumns().forEach(cm -> builder.addPartitionKeyColumn(cm.name, cm.type));
        legacy.staticColumns().forEach(cm -> builder.addStaticColumn(cm.name, cm.type.freeze()));
        legacy.clusteringColumns().forEach(cm -> builder.addClusteringColumn(cm.name, cm.type));
        legacy.regularColumns().forEach(cm -> builder.addRegularColumn(cm.name, cm.type.freeze()));
        return builder.build();
    }
}
