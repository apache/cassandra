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
package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "comparegossipandservicecache", description = "compares the Gossip and Storage service cache; returns true if they are in sync, false otherwise")
public class CompareGossipAndServiceCache extends NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        /** Cassandra maintains the Gossip info (Token, Status, etc.) in two caches 1) Gossip cache 2) Storage Service cache
         * The source of truth is the Gossip cache, which then updates the Storage service cache - but there exists no guarantee.
         * As a result, a wide variety of problems could occur, and one of the problems is a node could see different token ownership
         * than its peers.
         */
        StringBuilder sb = new StringBuilder();
        sb.append("Mismatch: " + probe.compareGossipAndStorageServiceCache());
        System.out.println(sb);
    }
}
