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

import java.io.PrintStream;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.google.common.collect.ArrayListMultimap;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;
import org.apache.cassandra.utils.LocalizeString;

import static java.util.stream.Collectors.toMap;

@SuppressWarnings("UseOfSystemOutOrSystemErr")
@Command(name = "status", description = "Print cluster information (state, load, IDs, ...)")
public class Status extends NodeToolCmd
{
    @Arguments(usage = "[<keyspace>]", description = "The keyspace name")
    private String keyspace = null;

    @Option(title = "resolve_ip", name = { "-r", "--resolve-ip" }, description = "Show node domain names instead of IPs")
    private boolean resolveIp = false;

    @Option(title = "sort",
    name = { "-s", "--sort" },
    description = "Sort by one of 'ip', 'host', 'load', 'owns', 'id', 'rack', 'state' or 'token'. " +
                  "Default ordering is ascending for 'ip', 'host', 'id', 'token', 'rack' and descending for 'load', 'owns', 'state'. " +
                  "Sorting by token is possible only when cluster does not use vnodes. When using vnodes, default " +
                  "sorting is by id otherwise by token.",
    allowedValues = { "ip", "host", "load", "owns", "id", "rack", "state", "token" })
    private String sortBy = null;

    @Option(title = "sort_order",
    name = { "-o", "--order" },
    description = "Sorting order: 'asc' for ascending, 'desc' for descending.",
    allowedValues = { "asc", "desc" })
    private String sortOrder = null;

    private boolean isTokenPerNode = true;
    private Collection<String> joiningNodes, leavingNodes, movingNodes, liveNodes, unreachableNodes;
    private Map<String, String> loadMap, hostIDMap;
    private EndpointSnitchInfoMBean epSnitchInfo;

    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        PrintStream errOut = probe.output().err;

        SortBy sortBy = parseSortBy(this.sortBy, errOut);
        SortOrder sortOrder = parseSortOrder(this.sortOrder, errOut);

        joiningNodes = probe.getJoiningNodes(true);
        leavingNodes = probe.getLeavingNodes(true);
        movingNodes = probe.getMovingNodes(true);
        loadMap = probe.getLoadMap(true);
        Map<String, String> tokensToEndpoints = probe.getTokenToEndpointMap(true);
        liveNodes = probe.getLiveNodes(true);
        unreachableNodes = probe.getUnreachableNodes(true);
        hostIDMap = probe.getHostIdMap(true);
        epSnitchInfo = probe.getEndpointSnitchInfoProxy();

        StringBuilder errors = new StringBuilder();
        TableBuilder.SharedTable sharedTable = new TableBuilder.SharedTable("  ");

        Map<String, Float> ownerships = null;
        boolean hasEffectiveOwns = false;
        try
        {
            ownerships = probe.effectiveOwnershipWithPort(keyspace);
            hasEffectiveOwns = true;
        }
        catch (IllegalStateException e)
        {
            try
            {
                ownerships = probe.getOwnershipWithPort();
                errors.append("Note: ").append(e.getMessage()).append("%n");
            }
            catch (Exception ex)
            {
                errOut.printf("%nError: %s%n", ex.getMessage());
                System.exit(1);
            }
        }
        catch (IllegalArgumentException ex)
        {
            errOut.printf("%nError: %s%n", ex.getMessage());
            System.exit(1);
        }

        SortedMap<String, SetHostStatWithPort> dcs = NodeTool.getOwnershipByDcWithPort(probe, resolveIp, tokensToEndpoints, ownerships);

        int nodesOfTokens = tokensToEndpoints.values().size();

        // More tokens than nodes (aka vnodes)?
        if (hostIDMap.size() < nodesOfTokens)
            isTokenPerNode = false;

        if (sortBy == null)
        {
            if (isTokenPerNode)
                sortBy = SortBy.token;
            else
                sortBy = SortBy.id;
        }
        else if (!isTokenPerNode && sortBy == SortBy.token)
        {
            errOut.printf("%nError: Can not sort by token when there is not token per node.%n");
            System.exit(1);
        }
        else if (!resolveIp && sortBy == SortBy.host)
        {
            errOut.printf("%nError: Can not sort by host when there is not -r/--resolve-ip flag used.%n");
            System.exit(1);
        }

        // Datacenters
        for (Map.Entry<String, SetHostStatWithPort> dc : dcs.entrySet())
        {
            TableBuilder tableBuilder = sharedTable.next();
            addNodesHeader(hasEffectiveOwns, tableBuilder);

            ArrayListMultimap<InetAddressAndPort, HostStatWithPort> hostToTokens = ArrayListMultimap.create();
            for (HostStatWithPort stat : dc.getValue())
                hostToTokens.put(stat.endpointWithPort, stat);

            Map<String, List<Object>> data = new HashMap<>();
            for (InetAddressAndPort endpoint : hostToTokens.keySet())
            {
                Float owns = ownerships.get(endpoint.getHostAddressAndPort());
                List<HostStatWithPort> tokens = hostToTokens.get(endpoint);

                HostStatWithPort hostStatWithPort = tokens.get(0);
                String epDns = hostStatWithPort.ipOrDns(printPort);
                List<Object> nodeData = addNode(epDns, endpoint, owns, hostStatWithPort, tokens.size(), hasEffectiveOwns);
                data.put(epDns, nodeData);
            }

            for (Map.Entry<String, List<Object>> entry : sortBy.tokenPerNode(isTokenPerNode)
                                                               .sortOrder(sortOrder)
                                                               .sort(data)
                                                               .entrySet())
            {
                List<Object> values = entry.getValue();
                List<String> row = new ArrayList<>();
                for (int i = 1; i < values.size(); i++)
                    row.add((String) values.get(i));

                tableBuilder.add(row);
            }
        }

        Iterator<TableBuilder> results = sharedTable.complete().iterator();
        boolean first = true;
        for (Map.Entry<String, SetHostStatWithPort> dc : dcs.entrySet())
        {
            if (!first)
            {
                out.println();
            }
            first = false;
            String dcHeader = String.format("Datacenter: %s%n", dc.getKey());
            out.print(dcHeader);
            for (int i = 0; i < (dcHeader.length() - 1); i++) out.print('=');
            out.println();

            // Legend
            out.println("Status=Up/Down");
            out.println("|/ State=Normal/Leaving/Joining/Moving");
            TableBuilder dcTable = results.next();
            dcTable.printTo(out);
        }

        out.printf("%n" + errors);
    }

    private void addNodesHeader(boolean hasEffectiveOwns, TableBuilder tableBuilder)
    {
        String owns = hasEffectiveOwns ? "Owns (effective)" : "Owns";

        if (isTokenPerNode)
            tableBuilder.add("--", "Address", "Load", owns, "Host ID", "Token", "Rack");
        else
            tableBuilder.add("--", "Address", "Load", "Tokens", owns, "Host ID", "Rack");
    }

    private List<Object> addNode(String epDns, InetAddressAndPort addressAndPort, Float owns,
                                 HostStatWithPort hostStat, int size, boolean hasEffectiveOwns)
    {
        String endpoint = addressAndPort.getHostAddressAndPort();
        String status, state, load, strOwns, hostID, rack;
        if (liveNodes.contains(endpoint)) status = "U";
        else if (unreachableNodes.contains(endpoint)) status = "D";
        else status = "?";
        if (joiningNodes.contains(endpoint)) state = "J";
        else if (leavingNodes.contains(endpoint)) state = "L";
        else if (movingNodes.contains(endpoint)) state = "M";
        else state = "N";

        String statusAndState = status.concat(state);
        load = loadMap.getOrDefault(endpoint, "?");
        strOwns = owns != null && hasEffectiveOwns ? new DecimalFormat("##0.0%").format(owns) : "?";
        hostID = hostIDMap.get(endpoint);

        try
        {
            rack = epSnitchInfo.getRack(endpoint);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }

        if (isTokenPerNode)
            return List.of(addressAndPort, statusAndState, epDns, load, strOwns, hostID, hostStat.token, rack);
        else
            return List.of(addressAndPort, statusAndState, epDns, load, String.valueOf(size), strOwns, hostID, rack);
    }

    public enum SortOrder
    {
        asc,
        desc
    }

    public enum SortBy
    {
        state(true)
        {
            @Override
            public Map<String, List<Object>> sort(Map<String, List<Object>> data)
            {
                return sortInternal(data, this::compareByState);
            }
        },
        ip(false)
        {
            @Override
            public Map<String, List<Object>> sort(Map<String, List<Object>> data)
            {
                return sortInternal(data, this::compareByAddress);
            }
        },
        host(false)
        {
            @Override
            public Map<String, List<Object>> sort(Map<String, List<Object>> data)
            {
                return sortInternal(data, this::compareByHost);
            }
        },
        load(true)
        {
            @Override
            public Map<String, List<Object>> sort(Map<String, List<Object>> data)
            {
                return sortInternal(data, this::compareByLoad);
            }
        },
        owns(true)
        {
            @Override
            public Map<String, List<Object>> sort(Map<String, List<Object>> data)
            {
                return sortInternal(data, this::compareByOwns);
            }
        },
        id(false)
        {
            @Override
            public Map<String, List<Object>> sort(Map<String, List<Object>> data)
            {
                return sortInternal(data, this::compareById);
            }
        },
        token(false)
        {
            @Override
            public Map<String, List<Object>> sort(Map<String, List<Object>> data)
            {
                return sortInternal(data, this::compareByToken);
            }
        },
        rack(false)
        {
            @Override
            public Map<String, List<Object>> sort(Map<String, List<Object>> data)
            {
                return sortInternal(data, this::compareByRack);
            }
        };

        private final boolean descendingByDefault;
        boolean tokenPerNode;
        SortOrder sortOrder;

        SortBy(boolean descendingByDefault)
        {
            this.descendingByDefault = descendingByDefault;
        }

        public abstract Map<String, List<Object>> sort(Map<String, List<Object>> data);

        boolean descending(SortOrder sortOrder)
        {
            return sortOrder == null ? descendingByDefault : sortOrder == SortOrder.desc;
        }

        SortBy sortOrder(SortOrder sortOrder)
        {
            this.sortOrder = sortOrder;
            return this;
        }

        SortBy tokenPerNode(boolean tokenPerNode)
        {
            this.tokenPerNode = tokenPerNode;
            return this;
        }

        int compareByState(Map.Entry<String, List<Object>> row1, Map.Entry<String, List<Object>> row2)
        {
            String str1 = (String) row1.getValue().get(1);
            String str2 = (String) row2.getValue().get(1);
            return evaluateComparision(str1.compareTo(str2));
        }

        int compareByHost(Map.Entry<String, List<Object>> row1, Map.Entry<String, List<Object>> row2)
        {
            String str1 = (String) row1.getValue().get(2);
            String str2 = (String) row2.getValue().get(2);
            return evaluateComparision(str1.compareTo(str2));
        }

        int compareByOwns(Map.Entry<String, List<Object>> row1, Map.Entry<String, List<Object>> row2)
        {
            String str1 = (String) row1.getValue().get(tokenPerNode ? 4 : 5);
            String str2 = (String) row2.getValue().get(tokenPerNode ? 4 : 5);

            Optional<Integer> maybeReturn = maybeCompareQuestionMarks(str1, str2);
            if (maybeReturn.isPresent())
                return maybeReturn.get();

            double value1 = Double.parseDouble(str1.replace("%", ""));
            double value2 = Double.parseDouble(str2.replace("%", ""));

            return evaluateComparision(Double.compare(value1, value2));
        }

        int compareByAddress(Map.Entry<String, List<Object>> row1, Map.Entry<String, List<Object>> row2)
        {
            InetAddressAndPort addr1 = (InetAddressAndPort) row1.getValue().get(0);
            InetAddressAndPort addr2 = (InetAddressAndPort) row2.getValue().get(0);

            return evaluateComparision(addr1.compareTo(addr2));
        }

        int compareById(Map.Entry<String, List<Object>> row1, Map.Entry<String, List<Object>> row2)
        {
            UUID id1;
            UUID id2;
            if (tokenPerNode)
            {
                id1 = UUID.fromString((String) row1.getValue().get(5));
                id2 = UUID.fromString((String) row2.getValue().get(5));
            }
            else
            {
                id1 = UUID.fromString((String) row1.getValue().get(6));
                id2 = UUID.fromString((String) row2.getValue().get(6));
            }
            return evaluateComparision(id1.compareTo(id2));
        }

        int compareByToken(Map.Entry<String, List<Object>> row1, Map.Entry<String, List<Object>> row2)
        {
            long token1 = Long.parseLong((String) row1.getValue().get(6));
            long token2 = Long.parseLong((String) row2.getValue().get(6));
            return evaluateComparision(Long.compare(token1, token2));
        }

        int compareByRack(Map.Entry<String, List<Object>> row1, Map.Entry<String, List<Object>> row2)
        {
            String rack1 = (String) row1.getValue().get(7);
            String rack2 = (String) row2.getValue().get(7);
            int byRack = evaluateComparision(rack1.compareTo(rack2));

            if (byRack != 0)
                return byRack;

            if (tokenPerNode)
                return compareByToken(row1, row2);
            else
                return compareById(row1, row2);
        }

        int compareByLoad(Map.Entry<String, List<Object>> row1, Map.Entry<String, List<Object>> row2)
        {
            String str1 = (String) row1.getValue().get(3);
            String str2 = (String) row2.getValue().get(3);

            Optional<Integer> maybeReturn = maybeCompareQuestionMarks(str1, str2);
            if (maybeReturn.isPresent())
                return maybeReturn.get();

            long value1 = FileUtils.parseFileSize(str1);
            long value2 = FileUtils.parseFileSize(str2);

            return evaluateComparision(Long.compare(value1, value2));
        }

        Map<String, List<Object>> sortInternal(Map<String, List<Object>> data,
                                               BiFunction<Map.Entry<String, List<Object>>, Map.Entry<String, List<Object>>, Integer> rowComparator)
        {
            return data.entrySet()
                       .stream()
                       .sorted(rowComparator::apply)
                       .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }

        private int evaluateComparision(int comparisionResult)
        {
            if (comparisionResult < 0)
                return descending(sortOrder) ? 1 : -1;
            if (comparisionResult > 0)
                return descending(sortOrder) ? -1 : 1;

            return 0;
        }

        private Optional<Integer> maybeCompareQuestionMarks(String str1, String str2)
        {
            // Check if str1 or str2 contains a '?' and set a value for it.
            boolean containsQuestionMark1 = str1.contains("?");
            boolean containsQuestionMark2 = str2.contains("?");
            // If both contain '?', return 0 (they are considered equal).
            if (containsQuestionMark1 && containsQuestionMark2)
                return Optional.of(0);

            // If str1 contains '?', ensure it's last (or first depending on descending).
            if (containsQuestionMark1)
                return Optional.of(descending(sortOrder) ? 1 : -1);

            // If str2 contains '?', ensure it's last (or first depending on descending).
            if (containsQuestionMark2)
                return Optional.of(descending(sortOrder) ? -1 : 1);

            return Optional.empty();
        }
    }

    private static SortBy parseSortBy(String setSortBy, PrintStream out)
    {
        if (setSortBy == null)
            return null;

        try
        {
            return SortBy.valueOf(LocalizeString.toLowerCaseLocalized(setSortBy));
        }
        catch (IllegalArgumentException ex)
        {
            String enabledValues = Arrays.stream(SortBy.values())
                                         .map(SortBy::name)
                                         .collect(Collectors.joining(", "));
            out.printf("%nError: Illegal value for -s/--sort used: '"
                       + setSortBy + "'. Supported values are " + enabledValues + ".%n");
            System.exit(1);
            return null;
        }
    }

    private static SortOrder parseSortOrder(String setSortOrder, PrintStream out)
    {
        if (setSortOrder == null)
            return null;

        try
        {
            return SortOrder.valueOf(LocalizeString.toLowerCaseLocalized(setSortOrder));
        }
        catch (IllegalArgumentException ex)
        {
            String enabledValues = Arrays.stream(SortOrder.values())
                                         .map(SortOrder::name)
                                         .collect(Collectors.joining(", "));
            out.printf("%nError: Illegal value for -o/--order used: '"
                       + setSortOrder + "'. Supported values are " + enabledValues + ".%n");
            System.exit(1);
            return null;
        }
    }
}
