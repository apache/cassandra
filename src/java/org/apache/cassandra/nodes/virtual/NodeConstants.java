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

import java.util.Set;

import com.google.common.collect.Sets;

import org.apache.cassandra.schema.SchemaConstants;

/**
 * This class is primarily in place to allow access to the table and view names without
 * needing to instantiate parts of the system that don't need to be instantiated.
 */
public class NodeConstants
{
    public static final String LOCAL = "local";
    public static final String PEERS_V2 = "peers_v2";
    public static final String LEGACY_PEERS = "peers";

    public static final String LOCAL_VIEW_NAME = "local_node";
    public static final String PEERS_V2_VIEW_NAME = "peer_v2_nodes";
    public static final String LEGACY_PEERS_VIEW_NAME = "peer_nodes";

    public static final Set<String> ALL_TABLES = Sets.newHashSet(LOCAL, PEERS_V2, LEGACY_PEERS);
    public static final Set<String> ALL_VIEWS = Sets.newHashSet(LOCAL_VIEW_NAME, PEERS_V2_VIEW_NAME, LEGACY_PEERS_VIEW_NAME);

    public static boolean canBeMapped(String keyspace, String table)
    {
        if (keyspace.equals(SchemaConstants.SYSTEM_VIEWS_KEYSPACE_NAME))
            return ALL_VIEWS.contains(table);
        if (keyspace.equals(SchemaConstants.SYSTEM_KEYSPACE_NAME))
            return ALL_TABLES.contains(table);
        return false;
    }

    public static String mapViewToTable(String view)
    {
        if (view.equals(LOCAL_VIEW_NAME))
            return LOCAL;
        else if (view.equals(PEERS_V2_VIEW_NAME))
            return PEERS_V2;
        else
            return LEGACY_PEERS;
    }

    public static String mapTableToView(String table)
    {
        if (table.equals(LOCAL))
            return LOCAL_VIEW_NAME;
        else if (table.equals(PEERS_V2))
            return PEERS_V2_VIEW_NAME;
        else
            return LEGACY_PEERS_VIEW_NAME;
    }
}
