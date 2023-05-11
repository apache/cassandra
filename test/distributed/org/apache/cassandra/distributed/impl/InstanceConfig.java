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

package org.apache.cassandra.distributed.impl;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;

import com.vdurmont.semver4j.Semver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.shared.Shared;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SimpleSeedProvider;

@Shared
public class InstanceConfig implements IInstanceConfig
{
    private static final Object NULL = new Object();
    private static final Logger logger = LoggerFactory.getLogger(InstanceConfig.class);

    public final int num;
    private final int jmxPort;

    public int num() { return num; }

    private final NetworkTopology networkTopology;
    public NetworkTopology networkTopology() { return networkTopology; }

    public final UUID hostId;
    public UUID hostId() { return hostId; }
    private final Map<String, Object> params = new TreeMap<>();
    private final Map<String, Object> dtestParams = new TreeMap<>();

    private final EnumSet featureFlags;

    private volatile InetAddressAndPort broadcastAddressAndPort;

    private InstanceConfig(int num,
                           NetworkTopology networkTopology,
                           String broadcast_address,
                           String listen_address,
                           String broadcast_rpc_address,
                           String rpc_address,
                           String seedIp,
                           String saved_caches_directory,
                           String[] data_file_directories,
                           String commitlog_directory,
                           String hints_directory,
                           String cdc_raw_directory,
                           String initial_token,
                           int jmx_port)
    {
        this.num = num;
        this.networkTopology = networkTopology;
        this.hostId = java.util.UUID.randomUUID();
        this    .set("num_tokens", 1)
                .set("broadcast_address", broadcast_address)
                .set("listen_address", listen_address)
                .set("broadcast_rpc_address", broadcast_rpc_address)
                .set("rpc_address", rpc_address)
                .set("saved_caches_directory", saved_caches_directory)
                .set("data_file_directories", data_file_directories)
                .set("commitlog_directory", commitlog_directory)
                .set("hints_directory", hints_directory)
                .set("cdc_raw_directory", cdc_raw_directory)
                .set("initial_token", initial_token)
                .set("partitioner", "org.apache.cassandra.dht.Murmur3Partitioner")
                .set("start_native_transport", true)
                .set("concurrent_writes", 2)
                .set("concurrent_counter_writes", 2)
                .set("concurrent_materialized_view_writes", 2)
                .set("concurrent_reads", 2)
                .set("memtable_flush_writers", 1)
                .set("concurrent_compactors", 1)
                .set("memtable_heap_space_in_mb", 10)
                .set("commitlog_sync", "batch")
                .set("storage_port", 7012)
                .set("endpoint_snitch", DistributedTestSnitch.class.getName())
                .set("seed_provider", new ParameterizedClass(SimpleSeedProvider.class.getName(),
                        Collections.singletonMap("seeds", seedIp)))
                .set("auto_bootstrap", false)
                // capacities that are based on `totalMemory` that should be fixed size
                .set("index_summary_capacity_in_mb", 50l)
                .set("counter_cache_size_in_mb", 50l)
                .set("key_cache_size_in_mb", 50l)
                // legacy parameters
                .forceSet("commitlog_sync_batch_window_in_ms", 1.0);
        this.featureFlags = EnumSet.noneOf(Feature.class);
        this.jmxPort = jmx_port;
    }

    private InstanceConfig(InstanceConfig copy)
    {
        this.num = copy.num;
        this.networkTopology = new NetworkTopology(copy.networkTopology);
        this.params.putAll(copy.params);
        this.dtestParams.putAll(copy.dtestParams);
        this.hostId = copy.hostId;
        this.featureFlags = copy.featureFlags;
        this.broadcastAddressAndPort = copy.broadcastAddressAndPort;
        this.jmxPort = copy.jmxPort;
    }


    @Override
    public InetSocketAddress broadcastAddress()
    {
        return DistributedTestSnitch.fromCassandraInetAddressAndPort(getBroadcastAddressAndPort());
    }

    protected InetAddressAndPort getBroadcastAddressAndPort()
    {
        if (broadcastAddressAndPort == null)
        {
            broadcastAddressAndPort = getAddressAndPortFromConfig("broadcast_address", "storage_port");
        }
        return broadcastAddressAndPort;
    }

    private InetAddressAndPort getAddressAndPortFromConfig(String addressProp, String portProp)
    {
        try
        {
            return InetAddressAndPort.getByNameOverrideDefaults(getString(addressProp), getInt(portProp));
        }
        catch (UnknownHostException e)
        {
            throw new IllegalStateException(e);
        }
    }

    public String localRack()
    {
        return networkTopology().localRack(broadcastAddress());
    }

    public String localDatacenter()
    {
        return networkTopology().localDC(broadcastAddress());
    }

    @Override
    public int jmxPort()
    {
        return this.jmxPort;
    }

    public InstanceConfig with(Feature featureFlag)
    {
        featureFlags.add(featureFlag);
        return this;
    }

    public InstanceConfig with(Feature... flags)
    {
        for (Feature flag : flags)
            featureFlags.add(flag);
        return this;
    }

    public boolean has(Feature featureFlag)
    {
        return featureFlags.contains(featureFlag);
    }

    public InstanceConfig set(String fieldName, Object value)
    {
        if (value == null)
            value = NULL;

        getParams(fieldName).put(fieldName, value);
        return this;
    }

    public InstanceConfig forceSet(String fieldName, Object value)
    {
        if (value == null)
            value = NULL;

        // test value
        getParams(fieldName).put(fieldName, value);
        return this;
    }

    private Map<String, Object> getParams(String fieldName)
    {
        Map<String, Object> map = params;
        if (fieldName.startsWith("dtest"))
            map = dtestParams;
        return map;
    }

    public void propagate(Object writeToConfig, Map<Class<?>, Function<Object, Object>> mapping)
    {
        throw new IllegalStateException("In-JVM dtests no longer support propagate");
    }

    public void validate()
    {
        if (((int) get("num_tokens")) > 1)
            throw new IllegalArgumentException("In-JVM dtests do not support vnodes as of now.");
    }

    public Object get(String name)
    {
        return getParams(name).get(name);
    }

    public int getInt(String name)
    {
        return (Integer) get(name);
    }

    public String getString(String name)
    {
        return (String) get(name);
    }

    public Map<String, Object> getParams()
    {
        return params;
    }

    public static InstanceConfig generate(int nodeNum, String ipAddress, NetworkTopology networkTopology, File root, String token, String seedIp, int datadirCount)
    {
        return new InstanceConfig(nodeNum,
                                  networkTopology,
                                  ipAddress,
                                  ipAddress,
                                  ipAddress,
                                  ipAddress,
                                  seedIp,
                                  String.format("%s/node%d/saved_caches", root, nodeNum),
                                  datadirs(datadirCount, root, nodeNum),
                                  String.format("%s/node%d/commitlog", root, nodeNum),
                                  String.format("%s/node%d/hints", root, nodeNum),
                                  String.format("%s/node%d/cdc", root, nodeNum),
                                  token,
                                  7199);
    }

    private static String[] datadirs(int datadirCount, File root, int nodeNum)
    {
        String datadirFormat = String.format("%s/node%d/data%%d", root.getPath(), nodeNum);
        String [] datadirs = new String[datadirCount];
        for (int i = 0; i < datadirs.length; i++)
            datadirs[i] = String.format(datadirFormat, i);
        return datadirs;
    }

    public InstanceConfig forVersion(Semver version)
    {
        return new InstanceConfig(this)
                            .set("seed_provider", new ParameterizedClass(SimpleSeedProvider.class.getName(),
                                                                         Collections.singletonMap("seeds", "127.0.0.1")));
    }

    public String toString()
    {
        return params.toString();
    }
}
