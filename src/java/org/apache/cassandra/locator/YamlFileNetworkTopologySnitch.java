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
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ResourceWatcher;
import org.apache.cassandra.utils.WrappedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Loader;
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

    /** Logger. */
    private static final Logger logger = LoggerFactory
            .getLogger(YamlFileNetworkTopologySnitch.class);

    /** Node data map, keyed by broadcast address. */
    private volatile Map<InetAddress, NodeData> nodeDataMap;

    /** Node data for this node. */
    private volatile NodeData localNodeData;

    /** Node data to fall back to when there is no match. */
    private volatile NodeData defaultNodeData;

    /** Default name for the topology configuration file. */
    private static final String DEFAULT_TOPOLOGY_CONFIG_FILENAME = "cassandra-topology.yaml";

    /** Name of the topology configuration file. */
    private final String topologyConfigFilename;

    /**
     * How often to check the topology configuration file, in milliseconds; defaults to one minute.
     */
    private final int checkPeriodInMs = 60 * 1000;

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
                    checkPeriodInMs);
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
     * Returns the preferred non-broadcast address for the endpoint, or null if none was specified.
     * <p>
     * Currently, the only preferred address that is considered is the data-center-local address.
     * </p>
     *
     * @param endpoint
     *            the broadcast address for the endpoint
     * @return the preferred non-broadcast address for the endpoint, or null if none was specified
     */
    private InetAddress getPreferredAddress(final InetAddress endpoint)
    {
        return getDcLocalAddress(endpoint);
    }

    /**
     * Returns the data-center-local address for the endpoint, or null if none was specified.
     *
     * @param endpoint
     *            the broadcast address for the endpoint
     * @return the data-center-local address for the endpoint, or null if none was specified
     */
    private InetAddress getDcLocalAddress(final InetAddress endpoint)
    {
        final NodeData nodeData = nodeDataMap.get(endpoint);
        return nodeData != null ? nodeData.dcLocalAddress : null;
    }

    /**
     * Reconnects to the endpoint through the preferred non-broadcast address if necessary.
     * <p>
     * A reconnect is performed if
     * <ul>
     * <li>the endpoint is not in the local data center and
     * <li>
     * <li>the endpoint has a configured preferred address as determined by {@link #getPreferredAddress(InetAddress)}.
     * </ul>
     * </p>
     *
     * @param endpoint
     *            the endpoint's broadcast address
     */
    private void reconnectViaPreferredAddress(final InetAddress endpoint)
    {
        if (!localNodeData.datacenter.equals(getDatacenter(endpoint)))
        {
            return;
        }

        reconnectViaPreferredAddress(endpoint, getPreferredAddress(endpoint));
    }

    /**
     * Reconnects to the endpoint through the preferred non-broadcast address if necessary.
     * <p>
     * A reconnect is performed to {@code preferredAddress} if the {@code preferredAddress} argument is not null.
     * </p>
     * <p>
     * This method is only meant to be called by {@link #reconnectViaPreferredAddress(InetAddress)}, but is declared to
     * have package-private scope in order to facilitate unit testing.
     * </p>
     *
     * @param endpoint
     *            the endpoint's broadcast address
     * @param preferredAddress
     *            the endpoint's preferred address to reconnect to
     */
    void reconnectViaPreferredAddress(final InetAddress endpoint,
            final InetAddress preferredAddress)
    {
        if (preferredAddress == null)
        {
            return;
        }

        MessagingService.instance().getConnectionPool(endpoint)
                .reset(preferredAddress);

        logger.debug(
                "Initiated reconnect to node with broadcast address {} using preferred address {}",
                endpoint, preferredAddress);
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
        final Yaml yaml = new Yaml(new Loader(configConstructor));
        final TopologyConfig topologyConfig = (TopologyConfig) yaml
                .load(configFileInputStream);

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
                    if (rack.rack_name == null)
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

        final IEndpointStateChangeSubscriber escs = new IEndpointStateChangeSubscriber()
        {

            /**
             * Called upon a "restart" gossip event; does nothing.
             *
             * @param endpoint
             *            the endpoint's broadcast address
             * @param state
             *            the endpoint's state
             */
            @Override
            public void onRestart(final InetAddress endpoint,
                    final EndpointState state)
            {
                // No-op
            }

            /**
             * Called upon a "remove" gossip event; does nothing.
             *
             * @param endpoint
             *            the endpoint's broadcast address
             */
            @Override
            public void onRemove(final InetAddress endpoint)
            {
                // No-op
            }

            /**
             * Called upon a "join" gossip event; attempts a reconnect to a preferred non-broadcast address if
             * necessary.
             *
             * @param endpoint
             *            the endpoint's broadcast address
             * @param epState
             *            the endpoint's state
             */
            @Override
            public void onJoin(final InetAddress endpoint,
                    final EndpointState epState)
            {
                reconnectViaPreferredAddress(endpoint);
            }

            /**
             * Called upon a "dead" gossip event; does nothing.
             *
             * @param endpoint
             *            the endpoint's broadcast address
             * @param state
             *            the endpoint's state
             */
            @Override
            public void onDead(final InetAddress endpoint,
                    final EndpointState state)
            {
                // No-op
            }

            /**
             * Called upon a "change" gossip event; does nothing.
             *
             * @param endpoint
             *            the endpoint's broadcast address
             * @param state
             *            the application state that has changed
             * @param value
             *            the new value of the state
             */
            @Override
            public void onChange(final InetAddress endpoint,
                    final ApplicationState state, final VersionedValue value)
            {
                // No-op
            }

            /**
             * Called upon an "alive" gossip event; attempts a reconnect to a preferred non-broadcast address if
             * necessary.
             *
             * @param endpoint
             *            the endpoint's broadcast address
             * @param state
             *            the endpoint's state
             */
            @Override
            public void onAlive(final InetAddress endpoint,
                    final EndpointState state)
            {
                reconnectViaPreferredAddress(endpoint);
            }
        };
        Gossiper.instance.register(escs);
    }

}
