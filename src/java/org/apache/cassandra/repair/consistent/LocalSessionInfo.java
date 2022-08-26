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

package org.apache.cassandra.repair.consistent;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

/**
 * helper for JMX management functions
 */
public class LocalSessionInfo
{
    public static final String SESSION_ID = "SESSION_ID";
    public static final String STATE = "STATE";
    public static final String STARTED = "STARTED";
    public static final String LAST_UPDATE = "LAST_UPDATE";
    public static final String COORDINATOR = "COORDINATOR";
    public static final String PARTICIPANTS = "PARTICIPANTS";
    public static final String PARTICIPANTS_WP = "PARTICIPANTS_WP";
    public static final String TABLES = "TABLES";


    private LocalSessionInfo() {}

    private static String tableString(TableId id)
    {
        TableMetadata meta = Schema.instance.getTableMetadata(id);
        return meta != null ? meta.keyspace + '.' + meta.name : "<null>";
    }

    static Map<String, String> sessionToMap(LocalSession session)
    {
        Map<String, String> m = new HashMap<>();
        m.put(SESSION_ID, session.sessionID.toString());
        m.put(STATE, session.getState().toString());
        m.put(STARTED, Long.toString(session.getStartedAt()));
        m.put(LAST_UPDATE, Long.toString(session.getLastUpdate()));
        m.put(COORDINATOR, session.coordinator.toString());
        m.put(PARTICIPANTS, Joiner.on(',').join(Iterables.transform(session.participants.stream().map(peer -> peer.getAddress()).collect(Collectors.toList()), InetAddress::getHostAddress)));
        m.put(PARTICIPANTS_WP, Joiner.on(',').join(Iterables.transform(session.participants, InetAddressAndPort::getHostAddressAndPort)));
        m.put(TABLES, Joiner.on(',').join(Iterables.transform(session.tableIds, LocalSessionInfo::tableString)));

        return m;
    }
}
