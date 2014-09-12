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
package org.apache.cassandra.locator;

import java.io.InputStream;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ResourceWatcher;
import org.apache.cassandra.utils.WrappedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import com.google.common.base.Objects;
import com.google.common.net.InetAddresses;

/**
 * Network topology snitch that reads its configuration from a YAML file.
 * <p>
 * This snitch supports connections over preferred addresses, such as a data-center-local address, based on the
 * reconnection trick used in {@link Ec2MultiRegionSnitch}. The configuration file, {@code cassandra-topology.yaml}, is
 * checked periodically for updates.
 * </p>
 */
public class YamlFileNetworkTopologySnitch
        extends AbstractNetworkTopologySnitch
{

    private static final Logger logger = LoggerFactory.getLogger(YamlFileNetworkTopologySnitch.class);
    
    /**
     * How often to check the topology configuration file, in milliseconds; defaults to one minute.
     */
    private static final int CHECK_PERIOD_IN_MS = 60 * 1000;

    /** Default name for the topology configuration file. */
    private static final String DEFAULT_TOPOLOGY_CONFIG_FILENAME = "cassandra-topology.yaml";

    /** Node data map, keyed by broadcast address. */
    private volatile Map<InetAddress, NodeData> nodeDataMap;

    /** Node data for this node. */
    private volatile NodeData localNodeData;

    /** Node data to fall back to when there is no match. */
    private volatile NodeData defaultNodeData;

    /** Name of the topology configuration file. */
    private final String topologyConfigFilename;

    /** True if the gossiper has been initialized. */
    private volatile boolean gossiperInitialized = false;

    /**
     * Constructor.
     *
     * @throws ConfigurationException
     *             on failure
     */
    public YamlFileNetworkTopologySnitch() throws ConfigurationException
    {
        this(DEFAULT_TOPOLOGY_CONFIG_FILENAME);
    }

    /**
     * Constructor.
     *
     * @param topologyConfigFilename
     *            name of the topology configuration file
     * @throws ConfigurationException
     *             on failure
     */
    YamlFileNetworkTopologySnitch(final String topologyConfigFilename)
            throws ConfigurationException
    {
        logger.warn("YamlFileNetworkTopologySnitch is deprecated; switch to GossipingPropertyFileSnitch instead");
        this.topologyConfigFilename = topologyConfigFilename;
        loadTopologyConfiguration();

        try
        {
            /*
             * Check if the topology configuration file is a plain file.
             */
            FBUtilities.resourceToFile(topologyConfigFilename);

            final Runnable runnable = new WrappedRunnable()
            {
                /**
                 * Loads the topology.
                 */
                protected void runMayThrow() throws ConfigurationException
                {
                    loadTopologyConfiguration();
                }
            };
            ResourceWatcher.watch(topologyConfigFilename, runnable,
                    CHECK_PERIOD_IN_MS);
        }
        catch (final ConfigurationException e)
        {
            logger.debug(
                    "{} found, but does not look like a plain file. Will not watch it for changes",
                    topologyConfigFilename);
        }
    }

    /**
     * Returns the name of the rack for the endpoint, or {@code UNKNOWN} if not known.
     *
     * @return the name of the data center for the endpoint, or {@code UNKNOWN} if not known
     */
    @Override
    public String getRack(final InetAddress endpoint)
    {
        final NodeData nodeData = nodeDataMap.get(endpoint);
        return nodeData != null ? nodeData.rack : defaultNodeData.rack;
    }

    /**
     * Returns the name of the data center for the endpoint, or {@code UNKNOWN} if not known.
     *
     * @return the name of the data center for the endpoint, or {@code UNKNOWN} if not known
     */
    @Override
    public String getDatacenter(final InetAddress endpoint)
    {
        final NodeData nodeData = nodeDataMap.get(endpoint);
        return nodeData != null ? nodeData.datacenter
                : defaultNodeData.datacenter;
    }

    /**
     * Root object type for the YAML topology configuration.
     */
    public static class TopologyConfig
    {
        public List<Datacenter> topology;
        public String default_dc_name = "UNKNOWN";
        public String default_rack_name = "UNKNOWN";
    }

    /**
     * Data center object type for the YAML topology configuration.
     */
    public static class Datacenter
    {
        public String dc_name;
        public List<Rack> racks = Collections.emptyList();
    }

    /**
     * Rack object type for the YAML topology configuration.
     */
    public static class Rack
    {
        public String rack_name;
        public List<Node> nodes = Collections.emptyList();
    }

    /**
     * Node object type for the YAML topology configuration.
     */
    public static class Node
    {
        public String broadcast_address;
        public String dc_local_address;
    }

    /**
     * Loads the topology configuration file.
     *
     * @throws ConfigurationException
     *             on failure
     */
    private synchronized void loadTopologyConfiguration()
            throws ConfigurationException
    {
        logger.debug("Loading topology configuration from {}",
                topologyConfigFilename);

        final TypeDescription topologyConfigTypeDescription = new TypeDescription(
                TopologyConfig.class);
        topologyConfigTypeDescription.putListPropertyType("topology",
                Datacenter.class);

        final TypeDescription topologyTypeDescription = new TypeDescription(
                Datacenter.class);
        topologyTypeDescription.putListPropertyType("racks", Rack.class);

        final TypeDescription rackTypeDescription = new TypeDescription(
                Rack.class);
        rackTypeDescription.putListPropertyType("nodes", Node.class);

        final Constructor configConstructor = new Constructor(
                TopologyConfig.class);
        configConstructor.addTypeDescription(topologyConfigTypeDescription);
        configConstructor.addTypeDescription(topologyTypeDescription);
        configConstructor.addTypeDescription(rackTypeDescription);

        final InputStream configFileInputStream = getClass().getClassLoader()
                .getResourceAsStream(topologyConfigFilename);
        if (configFileInputStream == null)
        {
            throw new ConfigurationException(
                    "Could not read topology config file "
                            + topologyConfigFilename);
        }
        Yaml yaml;
        TopologyConfig topologyConfig;
        try
        {
            yaml = new Yaml(configConstructor);
            topologyConfig = (TopologyConfig) yaml.load(configFileInputStream);
        }
        finally
        {
            FileUtils.closeQuietly(configFileInputStream);
        }
        final Map<InetAddress, NodeData> nodeDataMap = new HashMap<InetAddress, NodeData>();

        if (topologyConfig.topology == null)
        {
            throw new ConfigurationException(
                    "Topology configuration file is missing the topology section");
        }

        for (final Datacenter datacenter : topologyConfig.topology)
        {
            if (datacenter.dc_name == null)
            {
                throw new ConfigurationException(
                        "Topology configuration file is missing a data center name for some data center");
            }

            for (final Rack rack : datacenter.racks)
            {
                if (rack.rack_name == null)
                {
                    throw new ConfigurationException(
                            String.format(
                                    "Topology configuration file is missing a rack name for some rack under data center '%s'",
                                    datacenter.dc_name));
                }

                for (final Node node : rack.nodes)
                {
                    if (node.broadcast_address == null)
                    {
                        throw new ConfigurationException(
                                String.format(
                                        "Topology configuration file is missing a broadcast address for some node under data center '%s' rack '%s'",
                                        datacenter.dc_name, rack.rack_name));
                    }

                    final InetAddress endpoint = InetAddresses
                            .forString(node.broadcast_address);
                    final InetAddress dcLocalAddress = node.dc_local_address == null ? null
                            : InetAddresses.forString(node.dc_local_address);

                    final NodeData nodeData = new NodeData();
                    nodeData.datacenter = datacenter.dc_name;
                    nodeData.rack = rack.rack_name;
                    nodeData.dcLocalAddress = dcLocalAddress;

                    if (nodeDataMap.put(endpoint, nodeData) != null)
                    {
                        throw new ConfigurationException(
                                String.format(
                                        "IP address '%s' appears more than once in the topology configuration file",
                                        endpoint));
                    }

                    if (dcLocalAddress != null
                            && nodeDataMap.put(dcLocalAddress, nodeData) != null)
                    {
                        throw new ConfigurationException(
                                String.format(
                                        "IP address '%s' appears more than once in the topology configuration file",
                                        dcLocalAddress));
                    }
                }
            }
        }

        final NodeData localNodeData = nodeDataMap.get(FBUtilities
                .getBroadcastAddress());
        if (localNodeData == null)
        {
            throw new ConfigurationException(
                    "Topology configuration missing information for the local node");
        }

        final NodeData defaultNodeData = new NodeData();

        if (topologyConfig.default_dc_name == null)
        {
            throw new ConfigurationException(
                    "default_dc_name must be specified");
        }
        if (topologyConfig.default_rack_name == null)
        {
            throw new ConfigurationException(
                    "default_rack_name must be specified");
        }

        defaultNodeData.datacenter = topologyConfig.default_dc_name;
        defaultNodeData.rack = topologyConfig.default_rack_name;

        // YAML configuration looks good; now make the changes

        this.nodeDataMap = nodeDataMap;
        this.localNodeData = localNodeData;
        this.defaultNodeData = defaultNodeData;
        maybeSetApplicationState();

        if (logger.isDebugEnabled())
        {
            logger.debug(
                    "Built topology map from config file: localNodeData={}, nodeDataMap={}",
                    localNodeData, nodeDataMap);
        }

        if (gossiperInitialized)
        {
            StorageService.instance.gossipSnitchInfo();
        }
    }

    /**
     * be careful about just blindly updating ApplicationState.INTERNAL_IP everytime we read the yaml file,
     * as that can cause connections to get unnecessarily reset (via IESCS.onChange()).
     */
    private void maybeSetApplicationState()
    {
        if (localNodeData.dcLocalAddress == null)
            return;
        final EndpointState es = Gossiper.instance.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddress());
        if (es == null)
            return;
        final VersionedValue vv = es.getApplicationState(ApplicationState.INTERNAL_IP);
        if ((vv != null && !vv.value.equals(localNodeData.dcLocalAddress.getHostAddress()))
            || vv == null)
        {
            Gossiper.instance.addLocalApplicationState(ApplicationState.INTERNAL_IP,
                StorageService.instance.valueFactory.internalIP(localNodeData.dcLocalAddress.getHostAddress()));
        }
    }

    /**
     * Topology data for a node.
     */
    private class NodeData
    {
        /** Data center name. */
        public String datacenter;
        /** Rack name. */
        public String rack;
        /** Data-center-local address. */
        public InetAddress dcLocalAddress;

        /**
         * Returns a simple key-value string representation of this node's data.
         *
         * @return a simple key-value string representation of this node's data
         */
        public String toString()
        {
            return Objects.toStringHelper(this).add("datacenter", datacenter)
                    .add("rack", rack).add("dcLocalAddress", dcLocalAddress)
                    .toString();
        }
    }

    /**
     * Called in preparation for the initiation of the gossip loop.
     */
    @Override
    public synchronized void gossiperStarting()
    {
        gossiperInitialized = true;
        StorageService.instance.gossipSnitchInfo();
        Gossiper.instance.register(new ReconnectableSnitchHelper(this, localNodeData.datacenter, true));
    }

}
