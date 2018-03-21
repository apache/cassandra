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
package org.apache.cassandra.db.virtual;


import static org.apache.cassandra.gms.ApplicationState.NATIVE_ADDRESS_AND_PORT;
import static org.apache.cassandra.gms.ApplicationState.SCHEMA;
import static org.apache.cassandra.gms.ApplicationState.STATUS_WITH_PORT;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.InMemoryVirtualTable;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableMetadata;

public class RingState extends InMemoryVirtualTable {

    private static final String RPC_READY = "rpc_ready";
    private static final String HOST_ID = "host_id";
    private static final String NET_VERSION = "net_version";
    private static final String NATIVE_ADDRESS = "native_address";
    private static final String RELEASE_VERSION = "release_version";
    private static final String RACK = "rack";
    private static final String DC = "dc";
    private static final String SCHEMA_VERSION = "schema_version";
    private static final String LOAD = "load";
    private static final String STATUS = "status";
    private static final String NAME = "name";
    private static final String PORT = "port";
    private static final String IP = "ip";

    static
    {
        Map<String, CQL3Type> definitions = new HashMap<>();
        definitions.put(IP, CQL3Type.Native.TEXT);
        definitions.put(PORT, CQL3Type.Native.INT);
        definitions.put(NAME, CQL3Type.Native.TEXT);
        definitions.put(STATUS, CQL3Type.Native.TEXT);
        definitions.put(LOAD, CQL3Type.Native.TEXT);
        definitions.put(SCHEMA_VERSION, CQL3Type.Native.TEXT);
        definitions.put(DC, CQL3Type.Native.TEXT);
        definitions.put(RACK, CQL3Type.Native.TEXT);
        definitions.put(RELEASE_VERSION, CQL3Type.Native.TEXT);
        definitions.put(NATIVE_ADDRESS, CQL3Type.Native.TEXT);
        definitions.put(NET_VERSION, CQL3Type.Native.TEXT);
        definitions.put(HOST_ID, CQL3Type.Native.TEXT);
        definitions.put(RPC_READY, CQL3Type.Native.TEXT);

        schemaBuilder(definitions)
                .addKey(IP)
                .addKey(PORT)
                .register();
    }

    public RingState(TableMetadata metadata)
    {
        super(metadata);
    }

    public void read(StatementRestrictions restrictions, QueryOptions options, ResultBuilder result)
    {
        Set<Entry<InetAddressAndPort, EndpointState>> currentRingState = Gossiper.instance.getEndpointStates();
        // We should pull the columns from the query state/options
        for(Entry<InetAddressAndPort, EndpointState> entry : currentRingState)
        {
            EndpointState endpoint = entry.getValue();
            result.row(entry.getKey().address.getHostAddress(), entry.getKey().port)
                .column(NAME, entry.getKey().address.getHostName())
                .column(STATUS, endpoint.getApplicationState(STATUS_WITH_PORT).value)
                .column(LOAD, endpoint.getApplicationState(ApplicationState.LOAD).value)
                .column(SCHEMA_VERSION, endpoint.getApplicationState(SCHEMA).value)
                .column(DC, endpoint.getApplicationState(ApplicationState.DC).value)
                .column(RACK, endpoint.getApplicationState(ApplicationState.RACK).value)
                .column(RELEASE_VERSION, endpoint.getApplicationState(ApplicationState.RELEASE_VERSION).value)
                .column(NATIVE_ADDRESS, endpoint.getApplicationState(NATIVE_ADDRESS_AND_PORT).value)
                .column(NET_VERSION, endpoint.getApplicationState(ApplicationState.NET_VERSION).value)
                .column(HOST_ID, endpoint.getApplicationState(ApplicationState.HOST_ID).value)
                .column(RPC_READY, endpoint.getApplicationState(ApplicationState.RPC_READY).value)
                .endRow();
        }
    }
}
