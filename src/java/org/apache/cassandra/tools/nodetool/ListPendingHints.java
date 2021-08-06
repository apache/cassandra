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

import java.net.UnknownHostException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import io.airlift.airline.Command;
import org.apache.cassandra.hints.PendingHintsInfo;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

@Command(name = "listpendinghints", description = "Print all pending hints that this node has")
public class ListPendingHints extends NodeTool.NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        List<Map<String, String>> pendingHints = probe.listPendingHints();
        if(pendingHints.isEmpty())
        {
            probe.output().out.println("This node does not have any pending hints");
        }
        else
        {
            Map<String, String> endpointMap = probe.getHostIdToEndpointWithPort();
            Map<String, String> simpleStates = probe.getSimpleStatesWithPort();
            EndpointSnitchInfoMBean epSnitchInfo = probe.getEndpointSnitchInfoProxy();

            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");
            TableBuilder tableBuilder = new TableBuilder();

            tableBuilder.add("Host ID", "Address", "Rack", "DC", "Status", "Total files", "Newest", "Oldest");
            for (Map<String, String> hintInfo : pendingHints)
            {
                String endpoint = hintInfo.get(PendingHintsInfo.HOST_ID);
                String totalFiles = hintInfo.get(PendingHintsInfo.TOTAL_FILES);
                LocalDateTime newest = Instant.ofEpochMilli(Long.parseLong(hintInfo.get(PendingHintsInfo.NEWEST_TIMESTAMP)))
                                              .atZone(ZoneId.of("UTC"))
                                              .toLocalDateTime();
                LocalDateTime oldest = Instant.ofEpochMilli(Long.parseLong(hintInfo.get(PendingHintsInfo.OLDEST_TIMESTAMP)))
                                              .atZone(ZoneId.of("UTC"))
                                              .toLocalDateTime();
                String address = endpointMap.get(endpoint);
                String rack = null;
                String dc = null;
                String status = null;
                try
                {
                    rack = epSnitchInfo.getRack(address);
                    dc = epSnitchInfo.getDatacenter(address);
                    status = simpleStates.getOrDefault(InetAddressAndPort.getByName(address).toString(),
                                                       "Unknown");
                }
                catch (UnknownHostException e)
                {
                    rack = rack != null ? rack : "Unknown";
                    dc = dc != null ? dc : "Unknown";
                    status = "Unknown";
                }

                tableBuilder.add(endpoint,
                                 address,
                                 rack,
                                 dc,
                                 status,
                                 String.valueOf(totalFiles),
                                 dtf.format(newest),
                                 dtf.format(oldest));
            }
            tableBuilder.printTo(probe.output().out);
        }
    }
}