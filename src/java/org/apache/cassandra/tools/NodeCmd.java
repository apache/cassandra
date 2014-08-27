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
package org.apache.cassandra.tools;

import java.io.*;
import java.lang.management.MemoryUsage;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Maps;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.cli.*;
import org.yaml.snakeyaml.Loader;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutorMBean;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.service.CacheServiceMBean;
import org.apache.cassandra.service.PBSPredictionResult;
import org.apache.cassandra.service.PBSPredictorMBean;
import org.apache.cassandra.service.StorageProxyMBean;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.Pair;

public class NodeCmd
{
    private static final String HISTORYFILE = "nodetool.history";
    private static final Pair<String, String> SNAPSHOT_COLUMNFAMILY_OPT = Pair.create("cf", "column-family");
    private static final Pair<String, String> HOST_OPT = Pair.create("h", "host");
    private static final Pair<String, String> PORT_OPT = Pair.create("p", "port");
    private static final Pair<String, String> USERNAME_OPT = Pair.create("u", "username");
    private static final Pair<String, String> PASSWORD_OPT = Pair.create("pw", "password");
    private static final Pair<String, String> TAG_OPT = Pair.create("t", "tag");
    private static final Pair<String, String> TOKENS_OPT = Pair.create("T", "tokens");
    private static final Pair<String, String> PRIMARY_RANGE_OPT = Pair.create("pr", "partitioner-range");
    private static final Pair<String, String> SNAPSHOT_REPAIR_OPT = Pair.create("snapshot", "with-snapshot");
    private static final Pair<String, String> LOCAL_DC_REPAIR_OPT = Pair.create("local", "in-local-dc");
    private static final Pair<String, String> START_TOKEN_OPT = Pair.create("st", "start-token");
    private static final Pair<String, String> END_TOKEN_OPT = Pair.create("et", "end-token");
    private static final Pair<String, String> UPGRADE_ALL_SSTABLE_OPT = Pair.create("a", "include-all-sstables");
    private static final Pair<String, String> NO_SNAPSHOT = Pair.create("ns", "no-snapshot");
    private static final Pair<String, String> CFSTATS_IGNORE_OPT = Pair.create("i", "ignore");
    private static final Pair<String, String> RESOLVE_IP = Pair.create("r", "resolve-ip");

    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 7199;

    private static final ToolOptions options = new ToolOptions();

    private final NodeProbe probe;

    static
    {
        options.addOption(SNAPSHOT_COLUMNFAMILY_OPT, true, "only take a snapshot of the specified column family");
        options.addOption(HOST_OPT,     true, "node hostname or ip address");
        options.addOption(PORT_OPT,     true, "remote jmx agent port number");
        options.addOption(USERNAME_OPT, true, "remote jmx agent username");
        options.addOption(PASSWORD_OPT, true, "remote jmx agent password");
        options.addOption(TAG_OPT,      true, "optional name to give a snapshot");
        options.addOption(TOKENS_OPT,   false, "display all tokens");
        options.addOption(PRIMARY_RANGE_OPT, false, "only repair the first range returned by the partitioner for the node");
        options.addOption(SNAPSHOT_REPAIR_OPT, false, "repair one node at a time using snapshots");
        options.addOption(LOCAL_DC_REPAIR_OPT, false, "only repair against nodes in the same datacenter");
        options.addOption(START_TOKEN_OPT, true, "token at which repair range starts");
        options.addOption(END_TOKEN_OPT, true, "token at which repair range ends");
        options.addOption(UPGRADE_ALL_SSTABLE_OPT, false, "includes sstables that are already on the most recent version during upgradesstables");
        options.addOption(NO_SNAPSHOT, false, "disables snapshot creation for scrub");
        options.addOption(CFSTATS_IGNORE_OPT, false, "ignore the supplied list of keyspace.columnfamiles in statistics");
        options.addOption(RESOLVE_IP, false, "show node domain names instead of IPs");
    }

    public NodeCmd(NodeProbe probe)
    {
        this.probe = probe;
    }

    private enum NodeCommand
    {
        CFHISTOGRAMS,
        CFSTATS,
        CLEANUP,
        CLEARSNAPSHOT,
        COMPACT,
        COMPACTIONSTATS,
        DECOMMISSION,
        DESCRIBECLUSTER,
        DISABLEBINARY,
        DISABLEGOSSIP,
        DISABLEHANDOFF,
        DISABLETHRIFT,
        DRAIN,
        ENABLEBINARY,
        ENABLEGOSSIP,
        ENABLEHANDOFF,
        ENABLETHRIFT,
        FLUSH,
        GETCOMPACTIONTHRESHOLD,
        GETCOMPACTIONTHROUGHPUT,
        GETSTREAMTHROUGHPUT,
        GETENDPOINTS,
        GETSSTABLES,
        GOSSIPINFO,
        HELP,
        INFO,
        INVALIDATEKEYCACHE,
        INVALIDATEROWCACHE,
        JOIN,
        MOVE,
        NETSTATS,
        PAUSEHANDOFF,
        PROXYHISTOGRAMS,
        REBUILD,
        REFRESH,
        REMOVETOKEN,
        REMOVENODE,
        REPAIR,
        RESUMEHANDOFF,
        RING,
        SCRUB,
        SETCACHECAPACITY,
        SETCOMPACTIONTHRESHOLD,
        SETCOMPACTIONTHROUGHPUT,
        SETSTREAMTHROUGHPUT,
        SETTRACEPROBABILITY,
        SNAPSHOT,
        STATUS,
        STATUSBINARY,
        STATUSTHRIFT,
        STOP,
        TPSTATS,
        UPGRADESSTABLES,
        VERSION,
        DESCRIBERING,
        RANGEKEYSAMPLE,
        REBUILD_INDEX,
        RESETLOCALSCHEMA,
        PREDICTCONSISTENCY,
        ENABLEBACKUP,
        DISABLEBACKUP,
        SETCACHEKEYSTOSAVE, 
        DATAAVAILABILITY
    }


    /**
     * Prints usage information to stdout.
     */
    private static void printUsage()
    {
        HelpFormatter hf = new HelpFormatter();
        StringBuilder header = new StringBuilder(512);
        header.append("\nAvailable commands\n");
        final NodeToolHelp ntHelp = loadHelp();
        Collections.sort(ntHelp.commands, new Comparator<NodeToolHelp.NodeToolCommand>() 
        {
            @Override
            public int compare(NodeToolHelp.NodeToolCommand o1, NodeToolHelp.NodeToolCommand o2) 
            {
                return o1.name.compareTo(o2.name);
            }
        });
        for(NodeToolHelp.NodeToolCommand cmd : ntHelp.commands)
            addCmdHelp(header, cmd);
        String usage = String.format("java %s --host <arg> <command>%n", NodeCmd.class.getName());
        hf.printHelp(usage, "", options, "");
        System.out.println(header.toString());
    }

    private static NodeToolHelp loadHelp()
    {
        final InputStream is = NodeCmd.class.getClassLoader().getResourceAsStream("org/apache/cassandra/tools/NodeToolHelp.yaml");
        assert is != null;

        try
        {
            final Constructor constructor = new Constructor(NodeToolHelp.class);
            TypeDescription desc = new TypeDescription(NodeToolHelp.class);
            desc.putListPropertyType("commands", NodeToolHelp.NodeToolCommand.class);
            final Yaml yaml = new Yaml(new Loader(constructor));
            return (NodeToolHelp)yaml.load(is);
        }
        finally
        {
            FileUtils.closeQuietly(is);
        }
    }

    private static void addCmdHelp(StringBuilder sb, NodeToolHelp.NodeToolCommand cmd)
    {
        sb.append("  ").append(cmd.name);
        // Ghetto indentation (trying, but not too hard, to not look too bad)
        if (cmd.name.length() <= 20)
            for (int i = cmd.name.length(); i < 22; ++i) sb.append(" ");
        sb.append(" - ").append(cmd.help);
  }


    /**
     * Write a textual representation of the Cassandra ring.
     *
     * @param outs
     *            the stream to write to
     */
    public void printRing(PrintStream outs, String keyspace)
    {
        Map<String, String> tokensToEndpoints = probe.getTokenToEndpointMap();
        LinkedHashMultimap<String, String> endpointsToTokens = LinkedHashMultimap.create();
        for (Map.Entry<String, String> entry : tokensToEndpoints.entrySet())
            endpointsToTokens.put(entry.getValue(), entry.getKey());

        int maxAddressLength = Collections.max(endpointsToTokens.keys(), new Comparator<String>() {
            @Override
            public int compare(String first, String second)
            {
                return ((Integer)first.length()).compareTo((Integer)second.length());
            }
        }).length();

        String formatPlaceholder = "%%-%ds  %%-12s%%-7s%%-8s%%-16s%%-20s%%-44s%%n";
        String format = String.format(formatPlaceholder, maxAddressLength);

        // Calculate per-token ownership of the ring
        Map<InetAddress, Float> ownerships;
        boolean keyspaceSelected;
        try
        {
            ownerships = probe.effectiveOwnership(keyspace);
            keyspaceSelected = true;
        }
        catch (IllegalStateException ex)
        {
            ownerships = probe.getOwnership();
            outs.printf("Note: Ownership information does not include topology; for complete information, specify a keyspace%n");
            keyspaceSelected = false;
        }
        try
        {
            outs.println();
            for (Entry<String, SetHostStat> entry : getOwnershipByDc(false, tokensToEndpoints, ownerships).entrySet())
                printDc(outs, format, entry.getKey(), endpointsToTokens, keyspaceSelected, entry.getValue());
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }

        if(DatabaseDescriptor.getNumTokens() > 1)
        {
            outs.println("  Warning: \"nodetool ring\" is used to output all the tokens of a node.");
            outs.println("  To view status related info of a node use \"nodetool status\" instead.\n");
        }
    }

    private void printDc(PrintStream outs, String format, String dc, LinkedHashMultimap<String, String> endpointsToTokens,
                         boolean keyspaceSelected, SetHostStat hoststats)
    {
        Collection<String> liveNodes = probe.getLiveNodes();
        Collection<String> deadNodes = probe.getUnreachableNodes();
        Collection<String> joiningNodes = probe.getJoiningNodes();
        Collection<String> leavingNodes = probe.getLeavingNodes();
        Collection<String> movingNodes = probe.getMovingNodes();
        Map<String, String> loadMap = probe.getLoadMap();

        outs.println("Datacenter: " + dc);
        outs.println("==========");

        // get the total amount of replicas for this dc and the last token in this dc's ring
        List<String> tokens = new ArrayList<String>();
        float totalReplicas = 0f;
        String lastToken = "";

        for (HostStat stat : hoststats)
        {
            tokens.addAll(endpointsToTokens.get(stat.endpoint.getHostAddress()));
            lastToken = tokens.get(tokens.size() - 1);
            if (stat.owns != null)
                totalReplicas += stat.owns;
        }

        if (keyspaceSelected)
            outs.print("Replicas: " + (int) totalReplicas + "\n\n");

        outs.printf(format, "Address", "Rack", "Status", "State", "Load", "Owns", "Token");

        if (hoststats.size() > 1)
            outs.printf(format, "", "", "", "", "", "", lastToken);
        else
            outs.println();

        for (HostStat stat : hoststats)
        {
            String endpoint = stat.endpoint.getHostAddress();
            String rack;
            try
            {
                rack = probe.getEndpointSnitchInfoProxy().getRack(endpoint);
            }
            catch (UnknownHostException e)
            {
                rack = "Unknown";
            }

            String status = liveNodes.contains(endpoint)
                    ? "Up"
                    : deadNodes.contains(endpoint)
                            ? "Down"
                            : "?";

            String state = "Normal";

            if (joiningNodes.contains(endpoint))
                state = "Joining";
            else if (leavingNodes.contains(endpoint))
                state = "Leaving";
            else if (movingNodes.contains(endpoint))
                state = "Moving";

            String load = loadMap.containsKey(endpoint)
                    ? loadMap.get(endpoint)
                    : "?";
            String owns = stat.owns != null ? new DecimalFormat("##0.00%").format(stat.owns) : "?";
            outs.printf(format, endpoint, rack, status, state, load, owns, stat.token);
        }
        outs.println();
    }

    public void printDataAvailability(PrintStream outs, String keyspace)
    {
        Map<String, String> tokensToEndpoints = probe.getTokenToEndpointMap();
        LinkedHashMultimap<String, String> endpointsToTokens = LinkedHashMultimap.create();
        for (Map.Entry<String, String> entry : tokensToEndpoints.entrySet())
            endpointsToTokens.put(entry.getValue(), entry.getKey());        

        String format = "%44s %44s %30s %40s%n";               

        // Calculate per-token ownership of the ring
        Map<InetAddress, Float> ownerships;
        boolean keyspaceSelected;
        try
        {
            ownerships = probe.effectiveOwnership(keyspace);
            keyspaceSelected = true;
        }
        catch (IllegalStateException ex)
        {
            ownerships = probe.getOwnership();
            outs.printf("Note: Ownership information does not include topology; for complete information, specify a keyspace%n");
            keyspaceSelected = false;
        }
        try
        {
            outs.println();
            for (Entry<String, SetHostStat> entry : getOwnershipByDc(false, tokensToEndpoints, ownerships).entrySet())
                printDcData(outs, format, entry.getKey(), endpointsToTokens, keyspaceSelected, entry.getValue());
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }

        if(DatabaseDescriptor.getNumTokens() > 1)
        {
            outs.println("  Warning: \"nodetool dataavailability\" is used to output all the tokens of a node.");
            outs.println("  To view status related info of a node use \"nodetool status\" instead.\n");
        }
    }

    private void printDcData(PrintStream outs, String format, String dc, LinkedHashMultimap<String, String> endpointsToTokens,
                         boolean keyspaceSelected, SetHostStat hoststats)
    {
        Collection<String> liveNodes = probe.getLiveNodes();        

        outs.println("Datacenter: " + dc);
        outs.println("==========");

        // get the total amount of replicas for this dc and the last token in this dc's ring
        List<String> tokens = new ArrayList<String>();
        float totalReplicas = 0f;
        String lastToken = "";

        for (HostStat stat : hoststats)
        {
            tokens.addAll(endpointsToTokens.get(stat.endpoint.getHostAddress()));
            lastToken = tokens.get(tokens.size() - 1);
            if (stat.owns != null)
                totalReplicas += stat.owns;
        }

        if (keyspaceSelected)
            outs.print("Replicas: " + (int) totalReplicas + "\n\n");

        outs.printf(format, "TokenStart", "TokenEnd", "Available on # Machines", "IPs");

        if (hoststats.size() > 1)
        	outs.printf(format, "", "", "", "" );
        else
            outs.println();

        int statindex=0;//apse
        for (HostStat stat : hoststats)
        {        	         
            int availableMachines=0;
            ArrayList<String> iplist = new ArrayList<String>();
            int statindexaux=0;                   
			HostStat statbefore = stat;
            for (HostStat stataux : hoststats)
            {            	
            	String endpointaux = "lalala";
            	if (totalReplicas == 1 || totalReplicas == 0f ) {
            		statindexaux = statindexaux + 1;  
            		break;
            	} else if (statindex + totalReplicas > hoststats.size()) {
           			if (hoststats.size() - statindexaux + 1 < totalReplicas) {
           				endpointaux = stataux.endpoint.getHostAddress();
           			} else if (statindexaux + totalReplicas < (hoststats.size() + totalReplicas) % statindex ) {           				
            			endpointaux = stataux.endpoint.getHostAddress();
            		} else if (statindex - statindexaux == 1) {            			
                		statbefore = stataux;                	
            		} else {
            			statindexaux = statindexaux + 1;  
            			continue;
            		}        
            	} else if (statindexaux < statindex+(int)totalReplicas && statindexaux >= statindex) {
            		endpointaux = stataux.endpoint.getHostAddress();
            	} else if (statindex - statindexaux == 1) {
            		statbefore = stataux;
            	} else {
            		statindexaux = statindexaux + 1;  
            		continue;
            	}
           	         	
            	if (liveNodes.contains(endpointaux)) {
            		availableMachines = availableMachines + 1;
            		iplist.add(endpointaux);
            	}
            	
            	statindexaux = statindexaux + 1;            
            }
            if (statindex==0){
            	outs.printf(format, lastToken, stat.token, availableMachines, iplist);
            } else {
            	outs.printf(format, statbefore.token, stat.token, availableMachines, iplist);
            }
            statindex = statindex + 1;
        }
        outs.println();        
    }

    
    private class ClusterStatus
    {
        String kSpace = null, format = null;
        int maxAddressLength;
        Collection<String> joiningNodes, leavingNodes, movingNodes, liveNodes, unreachableNodes;
        Map<String, String> loadMap, hostIDMap, tokensToEndpoints;
        EndpointSnitchInfoMBean epSnitchInfo;
        PrintStream outs;
        private final boolean resolveIp;

        ClusterStatus(PrintStream outs, String kSpace, boolean resolveIp)
        {
            this.kSpace = kSpace;
            this.outs = outs;
            this.resolveIp = resolveIp;
            joiningNodes = probe.getJoiningNodes();
            leavingNodes = probe.getLeavingNodes();
            movingNodes = probe.getMovingNodes();
            loadMap = probe.getLoadMap();
            tokensToEndpoints = probe.getTokenToEndpointMap();
            liveNodes = probe.getLiveNodes();
            unreachableNodes = probe.getUnreachableNodes();
            hostIDMap = probe.getHostIdMap();
            epSnitchInfo = probe.getEndpointSnitchInfoProxy();
        }

        private void printStatusLegend()
        {
            outs.println("Status=Up/Down");
            outs.println("|/ State=Normal/Leaving/Joining/Moving");
        }

        private String getFormat(boolean hasEffectiveOwns, boolean isTokenPerNode)
        {
            if (format == null)
            {
                StringBuffer buf = new StringBuffer();
                String addressPlaceholder = String.format("%%-%ds  ", maxAddressLength);
                buf.append("%s%s  ");                         // status
                buf.append(addressPlaceholder);               // address
                buf.append("%-9s  ");                         // load
                if (!isTokenPerNode)  buf.append("%-6s  ");   // "Tokens"
                if (hasEffectiveOwns) buf.append("%-16s  ");  // "Owns (effective)"
                else                  buf.append("%-5s  ");   // "Owns
                buf.append("%-36s  ");                        // Host ID
                if (isTokenPerNode)   buf.append("%-39s  ");  // token
                buf.append("%s%n");                           // "Rack"

                format = buf.toString();
            }

            return format;
        }

        private void printNode(String endpoint, Float owns, List<HostStat> tokens, boolean hasEffectiveOwns, boolean isTokenPerNode) throws UnknownHostException
        {
            String status, state, load, strOwns, hostID, rack, fmt;
            fmt = getFormat(hasEffectiveOwns, isTokenPerNode);
            if      (liveNodes.contains(endpoint))        status = "U";
            else if (unreachableNodes.contains(endpoint)) status = "D";
            else                                          status = "?";
            if      (joiningNodes.contains(endpoint))     state = "J";
            else if (leavingNodes.contains(endpoint))     state = "L";
            else if (movingNodes.contains(endpoint))      state = "M";
            else                                          state = "N";

            load = loadMap.containsKey(endpoint) ? loadMap.get(endpoint) : "?";
            strOwns = owns != null ? new DecimalFormat("##0.0%").format(owns) : "?";
            hostID = hostIDMap.get(endpoint);
            rack = epSnitchInfo.getRack(endpoint);

            String endpointDns = tokens.get(0).ipOrDns();
            if (isTokenPerNode)
            {
                outs.printf(fmt, status, state, endpointDns, load, strOwns, hostID, tokens.get(0).token, rack);
            }
            else
            {
                outs.printf(fmt, status, state, endpointDns, load, tokens.size(), strOwns, hostID, rack);
            }
        }

        private void printNodesHeader(boolean hasEffectiveOwns, boolean isTokenPerNode)
        {
            String fmt = getFormat(hasEffectiveOwns, isTokenPerNode);
            String owns = hasEffectiveOwns ? "Owns (effective)" : "Owns";

            if (isTokenPerNode)
                outs.printf(fmt, "-", "-", "Address", "Load", owns, "Host ID", "Token", "Rack");
            else
                outs.printf(fmt, "-", "-", "Address", "Load", "Tokens", owns, "Host ID", "Rack");
        }

        void findMaxAddressLength(Map<String, SetHostStat> dcs) {
            maxAddressLength = 0;
            for (Map.Entry<String, SetHostStat> dc : dcs.entrySet())
            {
                for (HostStat stat : dc.getValue()) {
                    maxAddressLength = Math.max(maxAddressLength, stat.ipOrDns().length());
                }
            }
        }

        void print() throws UnknownHostException
        {
            Map<InetAddress, Float> ownerships;
            boolean hasEffectiveOwns = false, isTokenPerNode = true;

            try
            {
                ownerships = probe.effectiveOwnership(kSpace);
                hasEffectiveOwns = true;
            }
            catch (IllegalStateException e)
            {
                ownerships = probe.getOwnership();
                outs.printf("Note: Ownership information does not include topology; for complete information, specify a keyspace%n");
            }

            // More tokens then nodes (aka vnodes)?
            if (tokensToEndpoints.values().size() < tokensToEndpoints.keySet().size())
                isTokenPerNode = false;

            Map<String, SetHostStat> dcs = getOwnershipByDc(resolveIp, tokensToEndpoints, ownerships);

            findMaxAddressLength(dcs);

            // Datacenters
            for (Map.Entry<String, SetHostStat> dc : dcs.entrySet())
            {
                String dcHeader = String.format("Datacenter: %s%n", dc.getKey());
                outs.printf(dcHeader);
                for (int i=0; i < (dcHeader.length() - 1); i++) outs.print('=');
                outs.println();

                printStatusLegend();
                printNodesHeader(hasEffectiveOwns, isTokenPerNode);

                ArrayListMultimap<InetAddress, HostStat> hostToTokens = ArrayListMultimap.create();
                for (HostStat stat : dc.getValue())
                    hostToTokens.put(stat.endpoint, stat);

                // Nodes
                for (InetAddress endpoint : hostToTokens.keySet())
                {
                    Float owns = ownerships.get(endpoint);
                    List<HostStat> tokens = hostToTokens.get(endpoint);
                    printNode(endpoint.getHostAddress(), owns, tokens, hasEffectiveOwns, isTokenPerNode);
                }
            }
        }
    }

    private Map<String, SetHostStat> getOwnershipByDc(boolean resolveIp, Map<String, String> tokenToEndpoint, 
                                                      Map<InetAddress, Float> ownerships) throws UnknownHostException
    {
        Map<String, SetHostStat> ownershipByDc = Maps.newLinkedHashMap();
        EndpointSnitchInfoMBean epSnitchInfo = probe.getEndpointSnitchInfoProxy();

        for (Entry<String, String> tokenAndEndPoint : tokenToEndpoint.entrySet())
        {
            String dc = epSnitchInfo.getDatacenter(tokenAndEndPoint.getValue());
            if (!ownershipByDc.containsKey(dc))
                ownershipByDc.put(dc, new SetHostStat(resolveIp));
            ownershipByDc.get(dc).add(tokenAndEndPoint.getKey(), tokenAndEndPoint.getValue(), ownerships);
        }

        return ownershipByDc;
    }

    static class SetHostStat implements Iterable<HostStat> {
        final List<HostStat> hostStats = new ArrayList<HostStat>();
        final boolean resolveIp;

        public SetHostStat(boolean resolveIp)
        {
            this.resolveIp = resolveIp;
        }

        public int size()
        {
            return hostStats.size();
        }

        @Override
        public Iterator<HostStat> iterator() {
            return hostStats.iterator();
        }

        public void add(String token, String host, Map<InetAddress, Float> ownerships) throws UnknownHostException {
            InetAddress endpoint = InetAddress.getByName(host);
            Float owns = ownerships.get(endpoint);
            hostStats.add(new HostStat(token, endpoint, resolveIp, owns));
        }
    }

    static class HostStat {
        public final InetAddress endpoint;
        public final boolean resolveIp;
        public final Float owns;
        public final String token;

        public HostStat(String token, InetAddress endpoint, boolean resolveIp, Float owns) 
        {
            this.token = token;
            this.endpoint = endpoint;
            this.resolveIp = resolveIp;
            this.owns = owns;
        }

        public String ipOrDns()
        {
            return (resolveIp) ? endpoint.getHostName() : endpoint.getHostAddress();
        }
    }

    /** Writes a table of cluster-wide node information to a PrintStream
     * @throws UnknownHostException */
    public void printClusterStatus(PrintStream outs, String keyspace, boolean resolveIp) throws UnknownHostException
    {
        new ClusterStatus(outs, keyspace, resolveIp).print();
    }

    public void printThreadPoolStats(PrintStream outs)
    {
        outs.printf("%-25s%10s%10s%15s%10s%18s%n", "Pool Name", "Active", "Pending", "Completed", "Blocked", "All time blocked");

        Iterator<Map.Entry<String, JMXEnabledThreadPoolExecutorMBean>> threads = probe.getThreadPoolMBeanProxies();
        while (threads.hasNext())
        {
            Entry<String, JMXEnabledThreadPoolExecutorMBean> thread = threads.next();
            String poolName = thread.getKey();
            JMXEnabledThreadPoolExecutorMBean threadPoolProxy = thread.getValue();
            outs.printf("%-25s%10s%10s%15s%10s%18s%n",
                        poolName,
                        threadPoolProxy.getActiveCount(),
                        threadPoolProxy.getPendingTasks(),
                        threadPoolProxy.getCompletedTasks(),
                        threadPoolProxy.getCurrentlyBlockedTasks(),
                        threadPoolProxy.getTotalBlockedTasks());
        }

        outs.printf("%n%-20s%10s%n", "Message type", "Dropped");
        for (Entry<String, Integer> entry : probe.getDroppedMessages().entrySet())
            outs.printf("%-20s%10s%n", entry.getKey(), entry.getValue());
    }

    /**
     * Write node information.
     *
     * @param outs the stream to write to
     */
    public void printInfo(PrintStream outs, ToolCommandLine cmd)
    {
        boolean gossipInitialized = probe.isInitialized();
        List<String> toks = probe.getTokens();

        // If there is just 1 token, print it now like we always have, otherwise,
        // require that -T/--tokens be passed (that output is potentially verbose).
        if (toks.size() == 1)
            outs.printf("%-17s: %s%n", "Token", toks.get(0));
        else if (!cmd.hasOption(TOKENS_OPT.left))
            outs.printf("%-17s: (invoke with -T/--tokens to see all %d tokens)%n", "Token", toks.size());

        outs.printf("%-17s: %s%n", "ID", probe.getLocalHostId());
        outs.printf("%-17s: %s%n", "Gossip active", gossipInitialized);
        outs.printf("%-17s: %s%n", "Thrift active", probe.isThriftServerRunning());
        outs.printf("%-17s: %s%n", "Native Transport active", probe.isNativeTransportRunning());
        outs.printf("%-17s: %s%n", "Load", probe.getLoadString());
        if (gossipInitialized)
            outs.printf("%-17s: %s%n", "Generation No", probe.getCurrentGenerationNumber());
        else
            outs.printf("%-17s: %s%n", "Generation No", 0);

        // Uptime
        long secondsUp = probe.getUptime() / 1000;
        outs.printf("%-17s: %d%n", "Uptime (seconds)", secondsUp);

        // Memory usage
        MemoryUsage heapUsage = probe.getHeapMemoryUsage();
        double memUsed = (double)heapUsage.getUsed() / (1024 * 1024);
        double memMax = (double)heapUsage.getMax() / (1024 * 1024);
        outs.printf("%-17s: %.2f / %.2f%n", "Heap Memory (MB)", memUsed, memMax);

        // Data Center/Rack
        outs.printf("%-17s: %s%n", "Data Center", probe.getDataCenter());
        outs.printf("%-17s: %s%n", "Rack", probe.getRack());

        // Exceptions
        outs.printf("%-17s: %s%n", "Exceptions", probe.getExceptionCount());

        CacheServiceMBean cacheService = probe.getCacheServiceMBean();

        // Key Cache: Hits, Requests, RecentHitRate, SavePeriodInSeconds
        outs.printf("%-17s: size %d (bytes), capacity %d (bytes), %d hits, %d requests, %.3f recent hit rate, %d save period in seconds%n",
                    "Key Cache",
                    cacheService.getKeyCacheSize(),
                    cacheService.getKeyCacheCapacityInBytes(),
                    cacheService.getKeyCacheHits(),
                    cacheService.getKeyCacheRequests(),
                    cacheService.getKeyCacheRecentHitRate(),
                    cacheService.getKeyCacheSavePeriodInSeconds());

        // Row Cache: Hits, Requests, RecentHitRate, SavePeriodInSeconds
        outs.printf("%-17s: size %d (bytes), capacity %d (bytes), %d hits, %d requests, %.3f recent hit rate, %d save period in seconds%n",
                    "Row Cache",
                    cacheService.getRowCacheSize(),
                    cacheService.getRowCacheCapacityInBytes(),
                    cacheService.getRowCacheHits(),
                    cacheService.getRowCacheRequests(),
                    cacheService.getRowCacheRecentHitRate(),
                    cacheService.getRowCacheSavePeriodInSeconds());

        if (toks.size() > 1 && cmd.hasOption(TOKENS_OPT.left))
        {
            for (String tok : toks)
                outs.printf("%-17s: %s%n", "Token", tok);
        }
    }

    public void printReleaseVersion(PrintStream outs)
    {
        outs.println("ReleaseVersion: " + probe.getReleaseVersion());
    }

    public void printNetworkStats(final InetAddress addr, PrintStream outs)
    {
        outs.printf("Mode: %s%n", probe.getOperationMode());
        Set<InetAddress> hosts = addr == null ? probe.getStreamDestinations() : new HashSet<InetAddress>(){{add(addr);}};
        if (hosts.size() == 0)
            outs.println("Not sending any streams.");
        for (InetAddress host : hosts)
        {
            try
            {
                List<String> files = probe.getFilesDestinedFor(host);
                if (files.size() > 0)
                {
                    outs.printf("Streaming to: %s%n", host);
                    for (String file : files)
                        outs.printf("   %s%n", file);
                }
                else
                {
                    outs.printf(" Nothing streaming to %s%n", host);
                }
            }
            catch (IOException ex)
            {
                outs.printf("   Error retrieving file data for %s%n", host);
            }
        }

        hosts = addr == null ? probe.getStreamSources() : new HashSet<InetAddress>(){{add(addr); }};
        if (hosts.size() == 0)
            outs.println("Not receiving any streams.");
        for (InetAddress host : hosts)
        {
            try
            {
                List<String> files = probe.getIncomingFiles(host);
                if (files.size() > 0)
                {
                    outs.printf("Streaming from: %s%n", host);
                    for (String file : files)
                        outs.printf("   %s%n", file);
                }
                else
                {
                    outs.printf(" Nothing streaming from %s%n", host);
                }
            }
            catch (IOException ex)
            {
                outs.printf("   Error retrieving file data for %s%n", host);
            }
        }
        
        outs.printf("Read Repair Statistics:%nAttempted: %d%nMismatch (Blocking): %d%nMismatch (Background): %d%n", probe.getReadRepairAttempted(), probe.getReadRepairRepairedBlocking(), probe.getReadRepairRepairedBackground());

        MessagingServiceMBean ms = probe.msProxy;
        outs.printf("%-25s", "Pool Name");
        outs.printf("%10s", "Active");
        outs.printf("%10s", "Pending");
        outs.printf("%15s%n", "Completed");

        int pending;
        long completed;

        pending = 0;
        for (int n : ms.getCommandPendingTasks().values())
            pending += n;
        completed = 0;
        for (long n : ms.getCommandCompletedTasks().values())
            completed += n;
        outs.printf("%-25s%10s%10s%15s%n", "Commands", "n/a", pending, completed);

        pending = 0;
        for (int n : ms.getResponsePendingTasks().values())
            pending += n;
        completed = 0;
        for (long n : ms.getResponseCompletedTasks().values())
            completed += n;
        outs.printf("%-25s%10s%10s%15s%n", "Responses", "n/a", pending, completed);
    }

    public void printCompactionStats(PrintStream outs)
    {
        int compactionThroughput = probe.getCompactionThroughput();
        CompactionManagerMBean cm = probe.getCompactionManagerProxy();
        outs.println("pending tasks: " + cm.getPendingTasks());
        if (cm.getCompactions().size() > 0)
            outs.printf("%25s%16s%16s%16s%16s%10s%10s%n", "compaction type", "keyspace", "column family", "completed", "total", "unit", "progress");
        long remainingBytes = 0;
        for (Map<String, String> c : cm.getCompactions())
        {
            String percentComplete = new Long(c.get("total")) == 0
                                   ? "n/a"
                                   : new DecimalFormat("0.00").format((double) new Long(c.get("completed")) / new Long(c.get("total")) * 100) + "%";
            outs.printf("%25s%16s%16s%16s%16s%10s%10s%n", c.get("taskType"), c.get("keyspace"), c.get("columnfamily"), c.get("completed"), c.get("total"), c.get("unit"), percentComplete);
            if (c.get("taskType").equals(OperationType.COMPACTION.toString()))
                remainingBytes += (new Long(c.get("total")) - new Long(c.get("completed")));
        }
        long remainingTimeInSecs = compactionThroughput == 0 || remainingBytes == 0
                        ? -1
                        : (remainingBytes) / (long) (1024L * 1024L * compactionThroughput);
        String remainingTime = remainingTimeInSecs < 0
                        ? "n/a"
                        : String.format("%dh%02dm%02ds", remainingTimeInSecs / 3600, (remainingTimeInSecs % 3600) / 60, (remainingTimeInSecs % 60));

        outs.printf("%25s%10s%n", "Active compaction remaining time : ", remainingTime);
    }

    /**
     * Print the compaction threshold
     *
     * @param outs the stream to write to
     */
    public void printCompactionThreshold(PrintStream outs, String ks, String cf)
    {
        ColumnFamilyStoreMBean cfsProxy = probe.getCfsProxy(ks, cf);
        outs.println("Current compaction thresholds for " + ks + "/" + cf + ": \n" +
                     " min = " + cfsProxy.getMinimumCompactionThreshold() + ", " +
                     " max = " + cfsProxy.getMaximumCompactionThreshold());
    }

    /**
     * Print the compaction throughput
     *
     * @param outs the stream to write to
     */
    public void printCompactionThroughput(PrintStream outs)
    {
        outs.println("Current compaction throughput: " + probe.getCompactionThroughput() + " MB/s");
    }

    /**
     * Print the stream throughput
     *
     * @param outs the stream to write to
     */
    public void printStreamThroughput(PrintStream outs)
    {
        outs.println("Current stream throughput: " + probe.getStreamThroughput() + " MB/s");
    }

    /**
     * Print the name, snitch, partitioner and schema version(s) of a cluster
     *
     * @param outs Output stream
     * @param host Server address
     */
    public void printClusterDescription(PrintStream outs, String host)
    {
        // display cluster name, snitch and partitioner
        outs.println("Cluster Information:");
        outs.println("\tName: " + probe.getClusterName());
        outs.println("\tSnitch: " + probe.getEndpointSnitchInfoProxy().getSnitchName());
        outs.println("\tPartitioner: " + probe.getPartitioner());

        // display schema version for each node
        outs.println("\tSchema versions:");
        Map<String, List<String>> schemaVersions = probe.getSpProxy().getSchemaVersions();
        for (String version : schemaVersions.keySet())
        {
            outs.println(String.format("\t\t%s: %s%n", version, schemaVersions.get(version)));
        }
    }

    public void printColumnFamilyStats(PrintStream outs, boolean ignoreMode, String [] filterList)
    {
        OptionFilter filter = new OptionFilter(ignoreMode, filterList);
        Map <String, List <ColumnFamilyStoreMBean>> cfstoreMap = new HashMap <String, List <ColumnFamilyStoreMBean>>();

        // get a list of column family stores
        Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> cfamilies = probe.getColumnFamilyStoreMBeanProxies();

        while (cfamilies.hasNext())
        {
            Entry<String, ColumnFamilyStoreMBean> entry = cfamilies.next();
            String tableName = entry.getKey();
            ColumnFamilyStoreMBean cfsProxy = entry.getValue();

            if (!cfstoreMap.containsKey(tableName) && filter.isColumnFamilyIncluded(entry.getKey(), cfsProxy.getColumnFamilyName()))
            {
                List<ColumnFamilyStoreMBean> columnFamilies = new ArrayList<ColumnFamilyStoreMBean>();
                columnFamilies.add(cfsProxy);
                cfstoreMap.put(tableName, columnFamilies);
            }
            else if (filter.isColumnFamilyIncluded(entry.getKey(), cfsProxy.getColumnFamilyName()))
            {
                cfstoreMap.get(tableName).add(cfsProxy);
            }
        }

        // make sure all specified kss and cfs exist
        filter.verifyKeyspaces(probe.getKeyspaces());
        filter.verifyColumnFamilies();

        // print out the table statistics
        for (Entry<String, List<ColumnFamilyStoreMBean>> entry : cfstoreMap.entrySet())
        {
            String tableName = entry.getKey();
            List<ColumnFamilyStoreMBean> columnFamilies = entry.getValue();
            long tableReadCount = 0;
            long tableWriteCount = 0;
            int tablePendingTasks = 0;
            double tableTotalReadTime = 0.0f;
            double tableTotalWriteTime = 0.0f;

            outs.println("Keyspace: " + tableName);
            for (ColumnFamilyStoreMBean cfstore : columnFamilies)
            {
                long writeCount = cfstore.getWriteCount();
                long readCount = cfstore.getReadCount();

                if (readCount > 0)
                {
                    tableReadCount += readCount;
                    tableTotalReadTime += cfstore.getTotalReadLatencyMicros();
                }
                if (writeCount > 0)
                {
                    tableWriteCount += writeCount;
                    tableTotalWriteTime += cfstore.getTotalWriteLatencyMicros();
                }
                tablePendingTasks += cfstore.getPendingTasks();
            }

            double tableReadLatency = tableReadCount > 0 ? tableTotalReadTime / tableReadCount / 1000 : Double.NaN;
            double tableWriteLatency = tableWriteCount > 0 ? tableTotalWriteTime / tableWriteCount / 1000 : Double.NaN;

            outs.println("\tRead Count: " + tableReadCount);
            outs.println("\tRead Latency: " + String.format("%s", tableReadLatency) + " ms.");
            outs.println("\tWrite Count: " + tableWriteCount);
            outs.println("\tWrite Latency: " + String.format("%s", tableWriteLatency) + " ms.");
            outs.println("\tPending Tasks: " + tablePendingTasks);

            // print out column family statistics for this table
            for (ColumnFamilyStoreMBean cfstore : columnFamilies)
            {
                String cfName = cfstore.getColumnFamilyName();
                if(cfName.contains("."))
                    outs.println("\t\tColumn Family (index): " + cfName);
                else
                    outs.println("\t\tColumn Family: " + cfName);

                outs.println("\t\tSSTable count: " + cfstore.getLiveSSTableCount());
                int[] leveledSStables = cfstore.getSSTableCountPerLevel();
                if (leveledSStables != null)
                {
                    outs.print("\t\tSSTables in each level: [");
                    for (int level = 0; level < leveledSStables.length; level++)
                    {
                        int count = leveledSStables[level];
                        outs.print(count);
                        long maxCount = 4L; // for L0
                        if (level > 0)
                            maxCount = (long) Math.pow(10, level);
                        //  show max threshold for level when exceeded
                        if (count > maxCount)
                            outs.print("/" + maxCount);

                        if (level < leveledSStables.length - 1)
                            outs.print(", ");
                        else
                            outs.println("]");
                    }
                }
                outs.println("\t\tSpace used (live): " + cfstore.getLiveDiskSpaceUsed());
                outs.println("\t\tSpace used (total): " + cfstore.getTotalDiskSpaceUsed());
                outs.println("\t\tSSTable Compression Ratio: " + cfstore.getCompressionRatio());
                outs.println("\t\tNumber of Keys (estimate): " + cfstore.estimateKeys());
                outs.println("\t\tMemtable Columns Count: " + cfstore.getMemtableColumnsCount());
                outs.println("\t\tMemtable Data Size: " + cfstore.getMemtableDataSize());
                outs.println("\t\tMemtable Switch Count: " + cfstore.getMemtableSwitchCount());
                outs.println("\t\tRead Count: " + cfstore.getReadCount());
                outs.println("\t\tRead Latency: " + String.format("%01.3f", cfstore.getRecentReadLatencyMicros() / 1000) + " ms.");
                outs.println("\t\tWrite Count: " + cfstore.getWriteCount());
                outs.println("\t\tWrite Latency: " + String.format("%01.3f", cfstore.getRecentWriteLatencyMicros() / 1000) + " ms.");
                outs.println("\t\tPending Tasks: " + cfstore.getPendingTasks());
                outs.println("\t\tBloom Filter False Positives: " + cfstore.getBloomFilterFalsePositives());
                outs.println("\t\tBloom Filter False Ratio: " + String.format("%01.5f", cfstore.getRecentBloomFilterFalseRatio()));
                outs.println("\t\tBloom Filter Space Used: " + cfstore.getBloomFilterDiskSpaceUsed());
                outs.println("\t\tCompacted row minimum size: " + cfstore.getMinRowSize());
                outs.println("\t\tCompacted row maximum size: " + cfstore.getMaxRowSize());
                outs.println("\t\tCompacted row mean size: " + cfstore.getMeanRowSize());
                outs.println("\t\tAverage live cells per slice (last five minutes): " + cfstore.getLiveCellsPerSlice());
                outs.println("\t\tAverage tombstones per slice (last five minutes): " + cfstore.getTombstonesPerSlice());

                outs.println("");
            }
            outs.println("----------------");
        }
    }

    public void printRemovalStatus(PrintStream outs)
    {
        outs.println("RemovalStatus: " + probe.getRemovalStatus());
    }

    private void printCfHistograms(String keySpace, String columnFamily, PrintStream output)
    {
        ColumnFamilyStoreMBean store = this.probe.getCfsProxy(keySpace, columnFamily);

        // default is 90 offsets
        long[] offsets = new EstimatedHistogram().getBucketOffsets();

        long[] rrlh = store.getRecentReadLatencyHistogramMicros();
        long[] rwlh = store.getRecentWriteLatencyHistogramMicros();
        long[] sprh = store.getRecentSSTablesPerReadHistogram();
        long[] ersh = store.getEstimatedRowSizeHistogram();
        long[] ecch = store.getEstimatedColumnCountHistogram();

        output.println(String.format("%s/%s histograms", keySpace, columnFamily));

        output.println(String.format("%-10s%10s%18s%18s%18s%18s",
                                     "Offset", "SSTables", "Write Latency", "Read Latency", "Row Size", "Column Count"));

        for (int i = 0; i < offsets.length; i++)
        {
            output.println(String.format("%-10d%10s%18s%18s%18s%18s",
                                         offsets[i],
                                         (i < sprh.length ? sprh[i] : "0"),
                                         (i < rwlh.length ? rwlh[i] : "0"),
                                         (i < rrlh.length ? rrlh[i] : "0"),
                                         (i < ersh.length ? ersh[i] : "0"),
                                         (i < ecch.length ? ecch[i] : "0")));
        }
    }

    private void printProxyHistograms(PrintStream output)
    {
        StorageProxyMBean sp = this.probe.getSpProxy();
        long[] offsets = new EstimatedHistogram().getBucketOffsets();
        long[] rrlh = sp.getRecentReadLatencyHistogramMicros();
        long[] rwlh = sp.getRecentWriteLatencyHistogramMicros();
        long[] rrnglh = sp.getRecentRangeLatencyHistogramMicros();

        output.println("proxy histograms");
        output.println(String.format("%-10s%18s%18s%18s",
                                    "Offset", "Read Latency", "Write Latency", "Range Latency"));
        for (int i = 0; i < offsets.length; i++)
        {
            output.println(String.format("%-10d%18s%18s%18s",
                                        offsets[i],
                                        (i < rrlh.length ? rrlh[i] : "0"),
                                        (i < rwlh.length ? rwlh[i] : "0"),
                                        (i < rrnglh.length ? rrnglh[i] : "0")));
        }
    }

    private void printEndPoints(String keySpace, String cf, String key, PrintStream output)
    {
        List<InetAddress> endpoints = this.probe.getEndpoints(keySpace, cf, key);

        for (InetAddress anEndpoint : endpoints)
        {
           output.println(anEndpoint.getHostAddress());
        }
    }

    private void printSSTables(String keyspace, String cf, String key, PrintStream output)
    {
        List<String> sstables = this.probe.getSSTables(keyspace, cf, key);
        for (String sstable : sstables)
        {
            output.println(sstable);
        }
    }

    private void printIsNativeTransportRunning(PrintStream outs)
    {
        outs.println(probe.isNativeTransportRunning() ? "running" : "not running");
    }

    private void printIsThriftServerRunning(PrintStream outs)
    {
        outs.println(probe.isThriftServerRunning() ? "running" : "not running");
    }

    public void predictConsistency(Integer replicationFactor,
                                   Integer timeAfterWrite,
                                   Integer numVersions,
                                   Float percentileLatency,
                                   PrintStream output)
    {
        PBSPredictorMBean predictorMBean = probe.getPBSPredictorMBean();

        for(int r = 1; r <= replicationFactor; ++r) {
            for(int w = 1; w <= replicationFactor; ++w) {
                if(w+r > replicationFactor+1)
                    continue;

                try {
                    PBSPredictionResult result = predictorMBean.doPrediction(replicationFactor,
                                                                             r,
                                                                             w,
                                                                             timeAfterWrite,
                                                                             numVersions,
                                                                             percentileLatency);

                    if(r == 1 && w == 1) {
                        output.printf("%dms after a given write, with maximum version staleness of k=%d%n", timeAfterWrite, numVersions);
                    }

                    output.printf("N=%d, R=%d, W=%d%n", replicationFactor, r, w);
                    output.printf("Probability of consistent reads: %f%n", result.getConsistencyProbability());
                    output.printf("Average read latency: %fms (%.3fth %%ile %dms)%n", result.getAverageReadLatency(),
                                                                                   result.getPercentileReadLatencyPercentile()*100,
                                                                                   result.getPercentileReadLatencyValue());
                    output.printf("Average write latency: %fms (%.3fth %%ile %dms)%n%n", result.getAverageWriteLatency(),
                                                                                      result.getPercentileWriteLatencyPercentile()*100,
                                                                                      result.getPercentileWriteLatencyValue());
                } catch (Exception e) {
                        System.out.println(e.getMessage());
                        e.printStackTrace();
                        return;
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ConfigurationException, ParseException
    {
        CommandLineParser parser = new PosixParser();
        ToolCommandLine cmd = null;

        try
        {
            cmd = new ToolCommandLine(parser.parse(options, args));
        }
        catch (ParseException p)
        {
            badUse(p.getMessage());
        }

        String host = cmd.hasOption(HOST_OPT.left) ? cmd.getOptionValue(HOST_OPT.left) : DEFAULT_HOST;

        int port = DEFAULT_PORT;

        String portNum = cmd.getOptionValue(PORT_OPT.left);
        if (portNum != null)
        {
            try
            {
                port = Integer.parseInt(portNum);
            }
            catch (NumberFormatException e)
            {
                throw new ParseException("Port must be a number");
            }
        }

        String username = cmd.getOptionValue(USERNAME_OPT.left);
        String password = cmd.getOptionValue(PASSWORD_OPT.left);

        NodeProbe probe = null;
        try
        {
            probe = username == null ? new NodeProbe(host, port) : new NodeProbe(host, port, username, password);
        }
        catch (IOException ioe)
        {
            Throwable inner = findInnermostThrowable(ioe);
            if (inner instanceof ConnectException)
            {
                System.err.printf("Failed to connect to '%s:%d': %s%n", host, port, inner.getMessage());
                System.exit(1);
            }
            else if (inner instanceof UnknownHostException)
            {
                System.err.printf("Cannot resolve '%s': unknown host%n", host);
                System.exit(1);
            }
            else
            {
                err(ioe, "Error connecting to remote JMX agent!");
            }
        }
        try
        {
            //print history here after we've already determined we can reasonably call cassandra
            printHistory(args, cmd);
            NodeCommand command = null;

            try
            {
                command = cmd.getCommand();
            }
            catch (IllegalArgumentException e)
            {
                badUse(e.getMessage());
            }

            NodeCmd nodeCmd = new NodeCmd(probe);

            // Execute the requested command.
            String[] arguments = cmd.getCommandArguments();
            String tag;
            String columnFamilyName = null;

            switch (command)
            {
                case HELP : printUsage(); break;
                case RING :
                    if (arguments.length > 0) { nodeCmd.printRing(System.out, arguments[0]); }
                    else                      { nodeCmd.printRing(System.out, null); };
                    break;
                case DATAAVAILABILITY : nodeCmd.printDataAvailability(System.out, arguments[0]); break;                    
                case INFO            : nodeCmd.printInfo(System.out, cmd); break;
                case CFSTATS         :
                    boolean ignoreMode = cmd.hasOption(CFSTATS_IGNORE_OPT.left);
                    if (arguments.length > 0) { nodeCmd.printColumnFamilyStats(System.out, ignoreMode, arguments); }
                    else                      { nodeCmd.printColumnFamilyStats(System.out, false, null); }
                    break;
                case TPSTATS         : nodeCmd.printThreadPoolStats(System.out); break;
                case VERSION         : nodeCmd.printReleaseVersion(System.out); break;
                case COMPACTIONSTATS : nodeCmd.printCompactionStats(System.out); break;
                case DESCRIBECLUSTER : nodeCmd.printClusterDescription(System.out, host); break;
                case DISABLEBINARY   : probe.stopNativeTransport(); break;
                case ENABLEBINARY    : probe.startNativeTransport(); break;
                case STATUSBINARY    : nodeCmd.printIsNativeTransportRunning(System.out); break;
                case DISABLEGOSSIP   : probe.stopGossiping(); break;
                case ENABLEGOSSIP    : probe.startGossiping(); break;
                case DISABLEHANDOFF  : probe.disableHintedHandoff(); break;
                case ENABLEHANDOFF   : probe.enableHintedHandoff(); break;
                case PAUSEHANDOFF    : probe.pauseHintsDelivery(); break;
                case RESUMEHANDOFF   : probe.resumeHintsDelivery(); break;
                case DISABLETHRIFT   : probe.stopThriftServer(); break;
                case ENABLETHRIFT    : probe.startThriftServer(); break;
                case STATUSTHRIFT    : nodeCmd.printIsThriftServerRunning(System.out); break;
                case RESETLOCALSCHEMA: probe.resetLocalSchema(); break;
                case ENABLEBACKUP    : probe.setIncrementalBackupsEnabled(true); break;
                case DISABLEBACKUP   : probe.setIncrementalBackupsEnabled(false); break;
                    

                case STATUS :
                    boolean resolveIp = cmd.hasOption(RESOLVE_IP.left);
                    if (arguments.length > 0) nodeCmd.printClusterStatus(System.out, arguments[0], resolveIp);
                    else                      nodeCmd.printClusterStatus(System.out, null, resolveIp);
                    break;

                case DECOMMISSION :
                    if (arguments.length > 0)
                    {
                        System.err.println("Decommission will decommission the node you are connected to and does not take arguments!");
                        System.exit(1);
                    }
                    probe.decommission();
                    break;

                case DRAIN :
                    try { probe.drain(); }
                    catch (ExecutionException ee) { err(ee, "Error occured during flushing"); }
                    break;

                case NETSTATS :
                    if (arguments.length > 0) { nodeCmd.printNetworkStats(InetAddress.getByName(arguments[0]), System.out); }
                    else                      { nodeCmd.printNetworkStats(null, System.out); }
                    break;

                case SNAPSHOT :
                    columnFamilyName = cmd.getOptionValue(SNAPSHOT_COLUMNFAMILY_OPT.left);
                    /* FALL THRU */
                case CLEARSNAPSHOT :
                    tag = cmd.getOptionValue(TAG_OPT.left);
                    handleSnapshots(command, tag, arguments, columnFamilyName, probe);
                    break;

                case MOVE :
                    if (arguments.length != 1) { badUse("Missing token argument for move."); }
                    try
                    {
                        probe.move(arguments[0]);
                    }
                    catch (UnsupportedOperationException uoerror)
                    {
                        System.err.println(uoerror.getMessage());
                        System.exit(1);
                    }
                    break;

                case JOIN:
                    if (probe.isJoined())
                    {
                        System.err.println("This node has already joined the ring.");
                        System.exit(1);
                    }

                    probe.joinRing();
                    break;

                case SETCOMPACTIONTHROUGHPUT :
                    if (arguments.length != 1) { badUse("Missing value argument."); }
                    probe.setCompactionThroughput(Integer.parseInt(arguments[0]));
                    break;

                case SETSTREAMTHROUGHPUT :
                    if (arguments.length != 1) { badUse("Missing value argument."); }
                    probe.setStreamThroughput(Integer.parseInt(arguments[0]));
                    break;

                case SETTRACEPROBABILITY :
                    if (arguments.length != 1) { badUse("Missing value argument."); }
                    probe.setTraceProbability(Double.parseDouble(arguments[0]));
                    break;

                case REBUILD :
                    if (arguments.length > 1) { badUse("Too many arguments."); }
                    probe.rebuild(arguments.length == 1 ? arguments[0] : null);
                    break;

                case REMOVETOKEN :
                    System.err.println("Warn: removetoken is deprecated, please use removenode instead");
                case REMOVENODE  :
                    if (arguments.length != 1) { badUse("Missing an argument for removenode (either status, force, or an ID)"); }
                    else if (arguments[0].equals("status")) { nodeCmd.printRemovalStatus(System.out); }
                    else if (arguments[0].equals("force"))  { nodeCmd.printRemovalStatus(System.out); probe.forceRemoveCompletion(); }
                    else                                    { probe.removeNode(arguments[0]); }
                    break;

                case INVALIDATEKEYCACHE :
                    probe.invalidateKeyCache();
                    break;

                case INVALIDATEROWCACHE :
                    probe.invalidateRowCache();
                    break;

                case CLEANUP :
                case COMPACT :
                case REPAIR  :
                case FLUSH   :
                case SCRUB   :
                case UPGRADESSTABLES   :
                    optionalKSandCFs(command, cmd, arguments, probe);
                    break;

                case GETCOMPACTIONTHRESHOLD :
                    if (arguments.length != 2) { badUse("getcompactionthreshold requires ks and cf args."); }
                    nodeCmd.printCompactionThreshold(System.out, arguments[0], arguments[1]);
                    break;

                case GETCOMPACTIONTHROUGHPUT : nodeCmd.printCompactionThroughput(System.out); break;
                case GETSTREAMTHROUGHPUT : nodeCmd.printStreamThroughput(System.out); break;

                case CFHISTOGRAMS :
                    if (arguments.length != 2) { badUse("cfhistograms requires ks and cf args"); }
                    nodeCmd.printCfHistograms(arguments[0], arguments[1], System.out);
                    break;

                case SETCACHECAPACITY :
                    if (arguments.length != 2) { badUse("setcachecapacity requires key-cache-capacity, and row-cache-capacity args."); }
                    probe.setCacheCapacities(Integer.parseInt(arguments[0]), Integer.parseInt(arguments[1]));
                    break;

                case SETCACHEKEYSTOSAVE :
                    if (arguments.length != 2) { badUse("setcachekeystosave requires key-cache-keys-to-save, and row-cache-keys-to-save args."); }
                    probe.setCacheKeysToSave(Integer.parseInt(arguments[0]), Integer.parseInt(arguments[1]));
                    break;

                case SETCOMPACTIONTHRESHOLD :
                    if (arguments.length != 4) { badUse("setcompactionthreshold requires ks, cf, min, and max threshold args."); }
                    int minthreshold = Integer.parseInt(arguments[2]);
                    int maxthreshold = Integer.parseInt(arguments[3]);
                    if ((minthreshold < 0) || (maxthreshold < 0)) { badUse("Thresholds must be positive integers"); }
                    if (minthreshold > maxthreshold)              { badUse("Min threshold cannot be greater than max."); }
                    if (minthreshold < 2 && maxthreshold != 0)    { badUse("Min threshold must be at least 2"); }
                    probe.setCompactionThreshold(arguments[0], arguments[1], minthreshold, maxthreshold);
                    break;

                case GETENDPOINTS :
                    if (arguments.length != 3) { badUse("getendpoints requires ks, cf and key args"); }
                    nodeCmd.printEndPoints(arguments[0], arguments[1], arguments[2], System.out);
                    break;

                case PROXYHISTOGRAMS :
                    if (arguments.length != 0) { badUse("proxyhistograms does not take arguments"); }
                    nodeCmd.printProxyHistograms(System.out);
                    break;

                case GETSSTABLES:
                    if (arguments.length != 3) { badUse("getsstables requires ks, cf and key args"); }
                    nodeCmd.printSSTables(arguments[0], arguments[1], arguments[2], System.out);
                    break;

                case REFRESH:
                    if (arguments.length != 2) { badUse("load_new_sstables requires ks and cf args"); }
                    probe.loadNewSSTables(arguments[0], arguments[1]);
                    break;

                case REBUILD_INDEX:
                    if (arguments.length < 2) { badUse("rebuild_index requires ks and cf args"); }
                    if (arguments.length >= 3)
                        probe.rebuildIndex(arguments[0], arguments[1], arguments[2].split(","));
                    else
                        probe.rebuildIndex(arguments[0], arguments[1]);

                    break;

                case GOSSIPINFO : nodeCmd.printGossipInfo(System.out); break;

                case STOP:
                    if (arguments.length != 1) { badUse("stop requires a type."); }
                    probe.stop(arguments[0].toUpperCase());
                    break;

                case DESCRIBERING :
                    if (arguments.length != 1) { badUse("Missing keyspace argument for describering."); }
                    nodeCmd.printDescribeRing(arguments[0], System.out);
                    break;

                case RANGEKEYSAMPLE :
                    nodeCmd.printRangeKeySample(System.out);
                    break;

                case PREDICTCONSISTENCY:
                    if (arguments.length < 2) { badUse("Requires replication factor and time"); }
                    int numVersions = 1;
                    if (arguments.length == 3) { numVersions = Integer.parseInt(arguments[2]); }
                    float percentileLatency = .999f;
                    if (arguments.length == 4) { percentileLatency = Float.parseFloat(arguments[3]); }

                    nodeCmd.predictConsistency(Integer.parseInt(arguments[0]),
                                               Integer.parseInt(arguments[1]),
                                               numVersions,
                                               percentileLatency,
                                               System.out);
                    break;

                default :
                    throw new RuntimeException("Unreachable code.");
            }
        }
        finally
        {
            if (probe != null)
            {
                try
                {
                    probe.close();
                }
                catch (IOException ex)
                {
                    // swallow the exception so the user will see the real one.
                }
            }
        }
        System.exit(probe.isFailed() ? 1 : 0);
    }

    private static void printHistory(String[] args, ToolCommandLine cmd)
    {
        //don't bother to print if no args passed (meaning, nodetool is just printing out the sub-commands list)
        if (args.length == 0)
            return;
        String cmdLine = Joiner.on(" ").skipNulls().join(args);
        final String password = cmd.getOptionValue(PASSWORD_OPT.left);
        if (password != null)
            cmdLine = cmdLine.replace(password, "<hidden>");

        FileWriter writer = null;
        try
        {
            final String outputDir = FBUtilities.getToolsOutputDirectory().getCanonicalPath();
            writer = new FileWriter(new File(outputDir, HISTORYFILE), true);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
            writer.append(sdf.format(new Date()) + ": " + cmdLine + "\n");
        }
        catch (IOException ioe)
        {
            //quietly ignore any errors about not being able to write out history
        }
        finally
        {
            FileUtils.closeQuietly(writer);
        }
    }

    private static Throwable findInnermostThrowable(Throwable ex)
    {
        Throwable inner = ex.getCause();
        return inner == null ? ex : findInnermostThrowable(inner);
    }

    private void printDescribeRing(String keyspaceName, PrintStream out)
    {
        out.println("Schema Version:" + probe.getSchemaVersion());
        out.println("TokenRange: ");
        try
        {
            for (String tokenRangeString : probe.describeRing(keyspaceName))
            {
                out.println("\t" + tokenRangeString);
            }
        }
        catch (IOException e)
        {
            err(e, e.getMessage());
        }
    }

    private void printRangeKeySample(PrintStream outs)
    {
        outs.println("RangeKeySample: ");
        List<String> tokenStrings = this.probe.sampleKeyRange();
        for (String tokenString : tokenStrings)
        {
            outs.println("\t" + tokenString);
        }
    }

    private void printGossipInfo(PrintStream out) {
        out.println(probe.getGossipInfo());
    }

    private static void badUse(String useStr)
    {
        System.err.println(useStr);
        printUsage();
        System.exit(1);
    }

    private static void err(Exception e, String errStr)
    {
        System.err.println(errStr);
        e.printStackTrace();
        System.exit(3);
    }

    private static void complainNonzeroArgs(String[] args, NodeCommand cmd)
    {
        if (args.length > 0) {
            System.err.println("Too many arguments for command '"+cmd.toString()+"'.");
            printUsage();
            System.exit(1);
        }
    }

    private static void handleSnapshots(NodeCommand nc, String tag, String[] cmdArgs, String columnFamily, NodeProbe probe) throws InterruptedException, IOException
    {
        String[] keyspaces = Arrays.copyOfRange(cmdArgs, 0, cmdArgs.length);
        System.out.print("Requested " + ((nc == NodeCommand.SNAPSHOT) ? "creating" : "clearing") + " snapshot for: ");
        if ( keyspaces.length > 0 )
        {
          for (int i = 0; i < keyspaces.length; i++)
              System.out.print(keyspaces[i] + " ");
        }
        else
        {
            System.out.print("all keyspaces ");
        }

        if (columnFamily != null)
        {
            System.out.print("and column family: " + columnFamily);
        }
        System.out.println();

        switch (nc)
        {
            case SNAPSHOT :
                if (tag == null || tag.equals(""))
                    tag = new Long(System.currentTimeMillis()).toString();
                probe.takeSnapshot(tag, columnFamily, keyspaces);
                System.out.println("Snapshot directory: " + tag);
                break;
            case CLEARSNAPSHOT :
                probe.clearSnapshot(tag, keyspaces);
                break;
        }
    }

    private static void optionalKSandCFs(NodeCommand nc, ToolCommandLine cmd, String[] cmdArgs, NodeProbe probe) throws InterruptedException, IOException
    {
        // if there is one additional arg, it's the keyspace; more are columnfamilies
        List<String> keyspaces = cmdArgs.length == 0 ? probe.getKeyspaces() : Arrays.asList(cmdArgs[0]);
        for (String keyspace : keyspaces)
        {
            if (!probe.getKeyspaces().contains(keyspace))
            {
                System.err.println("Keyspace [" + keyspace + "] does not exist.");
                System.exit(1);
            }
        }

        // second loop so we're less likely to die halfway through due to invalid keyspace
        for (String keyspace : keyspaces)
        {
            String[] columnFamilies = cmdArgs.length <= 1 ? new String[0] : Arrays.copyOfRange(cmdArgs, 1, cmdArgs.length);
            switch (nc)
            {
                case REPAIR  :
                    boolean snapshot = cmd.hasOption(SNAPSHOT_REPAIR_OPT.left);
                    boolean localDC = cmd.hasOption(LOCAL_DC_REPAIR_OPT.left);
                    boolean primaryRange = cmd.hasOption(PRIMARY_RANGE_OPT.left);
                    if (cmd.hasOption(START_TOKEN_OPT.left) || cmd.hasOption(END_TOKEN_OPT.left))
                        probe.forceRepairRangeAsync(System.out, keyspace, snapshot, localDC, cmd.getOptionValue(START_TOKEN_OPT.left), cmd.getOptionValue(END_TOKEN_OPT.left), columnFamilies);
                    else
                        probe.forceRepairAsync(System.out, keyspace, snapshot, localDC, primaryRange, columnFamilies);
                    break;
                case FLUSH   :
                    try { probe.forceTableFlush(keyspace, columnFamilies); }
                    catch (ExecutionException ee) { err(ee, "Error occurred during flushing"); }
                    break;
                case COMPACT :
                    try { probe.forceTableCompaction(keyspace, columnFamilies); }
                    catch (ExecutionException ee) { err(ee, "Error occurred during compaction"); }
                    break;
                case CLEANUP :
                    if (keyspace.equals(Table.SYSTEM_KS)) { break; } // Skip cleanup on system cfs.
                    try { probe.forceTableCleanup(keyspace, columnFamilies); }
                    catch (ExecutionException ee) { err(ee, "Error occurred during cleanup"); }
                    break;
                case SCRUB :
                    boolean disableSnapshot = cmd.hasOption(NO_SNAPSHOT.left);
                    try { probe.scrub(disableSnapshot, keyspace, columnFamilies); }
                    catch (ExecutionException ee) { err(ee, "Error occurred while scrubbing keyspace " + keyspace); }
                    break;
                case UPGRADESSTABLES :
                    boolean excludeCurrentVersion = !cmd.hasOption(UPGRADE_ALL_SSTABLE_OPT.left);
                    try { probe.upgradeSSTables(keyspace, excludeCurrentVersion, columnFamilies); }
                    catch (ExecutionException ee) { err(ee, "Error occurred while upgrading the sstables for keyspace " + keyspace); }
                    break;
                default:
                    throw new RuntimeException("Unreachable code.");
            }
        }
    }

    /**
     * Used for filtering keyspaces and columnfamilies to be displayed using the cfstats command.
     */
    private static class OptionFilter
    {
        private Map<String, List<String>> filter = new HashMap<String, List<String>>();
        private Map<String, List<String>> verifier = new HashMap<String, List<String>>();
        private String [] filterList;
        private boolean ignoreMode;

        public OptionFilter(boolean ignoreMode, String... filterList)
        {
            this.filterList = filterList;
            this.ignoreMode = ignoreMode;

            if(filterList == null)
                return;

            for(String s : filterList)
            {
                String [] keyValues = s.split("\\.", 2);

                // build the map that stores the ks' and cfs to use
                if(!filter.containsKey(keyValues[0]))
                {
                    filter.put(keyValues[0], new ArrayList<String>());
                    verifier.put(keyValues[0], new ArrayList<String>());

                    if(keyValues.length == 2)
                    {
                        filter.get(keyValues[0]).add(keyValues[1]);
                        verifier.get(keyValues[0]).add(keyValues[1]);
                    }
                }
                else
                {
                    if(keyValues.length == 2)
                    {
                        filter.get(keyValues[0]).add(keyValues[1]);
                        verifier.get(keyValues[0]).add(keyValues[1]);
                    }
                }
            }
        }

        public boolean isColumnFamilyIncluded(String keyspace, String columnFamily)
        {
            // supplying empty params list is treated as wanting to display all kss & cfs
            if(filterList == null)
                return !ignoreMode;

            List<String> cfs = filter.get(keyspace);

            // no such keyspace is in the map
            if (cfs == null)
                return ignoreMode;
                // only a keyspace with no cfs was supplied
                // so ignore or include (based on the flag) every column family in specified keyspace
            else if (cfs.size() == 0)
                return !ignoreMode;

            // keyspace exists, and it contains specific cfs
            verifier.get(keyspace).remove(columnFamily);
            return ignoreMode ^ cfs.contains(columnFamily);
        }

        public void verifyKeyspaces(List<String> keyspaces)
        {
            for(String ks : verifier.keySet())
                if(!keyspaces.contains(ks))
                    throw new RuntimeException("Unknown keyspace: " + ks);
        }

        public void verifyColumnFamilies()
        {
            for(String ks : filter.keySet())
                if(verifier.get(ks).size() > 0)
                    throw new RuntimeException("Unknown column families: " + verifier.get(ks).toString() + " in keyspace: " + ks);
        }
    }

    private static class ToolOptions extends Options
    {
        public void addOption(Pair<String, String> opts, boolean hasArgument, String description)
        {
            addOption(opts, hasArgument, description, false);
        }

        public void addOption(Pair<String, String> opts, boolean hasArgument, String description, boolean required)
        {
            addOption(opts.left, opts.right, hasArgument, description, required);
        }

        public void addOption(String opt, String longOpt, boolean hasArgument, String description, boolean required)
        {
            Option option = new Option(opt, longOpt, hasArgument, description);
            option.setRequired(required);
            addOption(option);
        }
    }

    private static class ToolCommandLine
    {
        private final CommandLine commandLine;

        public ToolCommandLine(CommandLine commands)
        {
            commandLine = commands;
        }

        public Option[] getOptions()
        {
            return commandLine.getOptions();
        }

        public boolean hasOption(String opt)
        {
            return commandLine.hasOption(opt);
        }

        public String getOptionValue(String opt)
        {
            return commandLine.getOptionValue(opt);
        }

        public NodeCommand getCommand()
        {
            if (commandLine.getArgs().length == 0)
                throw new IllegalArgumentException("Command was not specified.");

            String command = commandLine.getArgs()[0];

            try
            {
                return NodeCommand.valueOf(command.toUpperCase());
            }
            catch (IllegalArgumentException e)
            {
                throw new IllegalArgumentException("Unrecognized command: " + command);
            }
        }

        public String[] getCommandArguments()
        {
            List params = commandLine.getArgList();

            if (params.size() < 2) // command parameters are empty
                return new String[0];

            String[] toReturn = new String[params.size() - 1];

            for (int i = 1; i < params.size(); i++)
            {
                String parm = (String) params.get(i);
                // why? look at CASSANDRA-4808
                if (parm.startsWith("\\"))
                    parm = parm.substring(1);
                toReturn[i - 1] = parm;
            }
            return toReturn;
        }
    }
}
