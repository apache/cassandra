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

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SimpleSeedProvider;
import org.apache.cassandra.locator.SimpleSnitch;

import java.io.File;
import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public class InstanceConfig implements IInstanceConfig
{
    private static final Object NULL = new Object();

    public final int num;
    public int num() { return num; }

    public final UUID hostId;
    public UUID hostId() { return hostId; }
    private final Map<String, Object> params = new TreeMap<>();

    private volatile InetAddressAndPort broadcastAddressAndPort;

    @Override
    public InetAddressAndPort broadcastAddressAndPort()
    {
        if (broadcastAddressAndPort == null)
        {
            try
            {
                broadcastAddressAndPort = InetAddressAndPort.getByNameOverrideDefaults(getString("broadcast_address"), getInt("storage_port"));
            }
            catch (UnknownHostException e)
            {
                throw new IllegalStateException(e);
            }
        }
        return broadcastAddressAndPort;
    }

    private InstanceConfig(int num,
                           String broadcast_address,
                           String listen_address,
                           String broadcast_rpc_address,
                           String rpc_address,
                           String saved_caches_directory,
                           String[] data_file_directories,
                           String commitlog_directory,
                           String hints_directory,
                           String cdc_raw_directory,
                           String initial_token)
    {
        this.num = num;
        this.hostId = java.util.UUID.randomUUID();
        this    .set("broadcast_address", broadcast_address)
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
                .set("concurrent_writes", 2)
                .set("concurrent_counter_writes", 2)
                .set("concurrent_materialized_view_writes", 2)
                .set("concurrent_reads", 2)
                .set("memtable_flush_writers", 1)
                .set("concurrent_compactors", 1)
                .set("memtable_heap_space_in_mb", 10)
                .set("commitlog_sync", "batch")
                .set("storage_port", 7010)
                .set("endpoint_snitch", SimpleSnitch.class.getName())
                .set("seed_provider", new ParameterizedClass(SimpleSeedProvider.class.getName(),
                        Collections.singletonMap("seeds", "127.0.0.1:7010")))
                // required settings for dtest functionality
                .set("diagnostic_events_enabled", true)
                // legacy parameters
                .forceSet("commitlog_sync_batch_window_in_ms", 1.0);
                //
    }

    private InstanceConfig(InstanceConfig copy)
    {
        this.num = copy.num;
        this.params.putAll(copy.params);
        this.hostId = copy.hostId;
    }

    public InstanceConfig set(String fieldName, Object value)
    {
        if (value == null)
            value = NULL;

        // test value
        propagate(new Config(), fieldName, value, false);
        params.put(fieldName, value);
        return this;
    }

    private InstanceConfig forceSet(String fieldName, Object value)
    {
        if (value == null)
            value = NULL;

        // test value
        params.put(fieldName, value);
        return this;
    }

    public void propagateIfSet(Object writeToConfig, String fieldName)
    {
        if (params.containsKey(fieldName))
            propagate(writeToConfig, fieldName, params.get(fieldName), true);
    }

    public void propagate(Object writeToConfig)
    {
        for (Map.Entry<String, Object> e : params.entrySet())
            propagate(writeToConfig, e.getKey(), e.getValue(), true);
    }

    private void propagate(Object writeToConfig, String fieldName, Object value, boolean ignoreMissing)
    {
        if (value == NULL)
            value = null;

        Class<?> configClass = writeToConfig.getClass();
        Field valueField;
        try
        {
            valueField = configClass.getDeclaredField(fieldName);
        }
        catch (NoSuchFieldException e)
        {
            if (!ignoreMissing)
                throw new IllegalStateException(e);
            return;
        }

        if (valueField.getType().isEnum() && value instanceof String)
        {
            String test = (String) value;
            value = Arrays.stream(valueField.getType().getEnumConstants())
                    .filter(e -> ((Enum<?>)e).name().equals(test))
                    .findFirst()
                    .get();
        }
        try
        {
            valueField.set(writeToConfig, value);
        }
        catch (IllegalAccessException e)
        {
            throw new IllegalStateException(e);
        }
        catch (IllegalArgumentException e)
        {
            throw new IllegalStateException(e);
        }
    }

    public Object get(String name)
    {
        return params.get(name);
    }

    public int getInt(String name)
    {
        return (Integer)params.get(name);
    }

    public String getString(String name)
    {
        return (String)params.get(name);
    }

    public static InstanceConfig generate(int nodeNum, File root, String token)
    {
        return new InstanceConfig(nodeNum,
                                  "127.0.0." + nodeNum,
                                  "127.0.0." + nodeNum,
                                  "127.0.0." + nodeNum,
                                  "127.0.0." + nodeNum,
                                  String.format("%s/node%d/saved_caches", root, nodeNum),
                                  new String[] { String.format("%s/node%d/data", root, nodeNum) },
                                  String.format("%s/node%d/commitlog", root, nodeNum),
                                  String.format("%s/node%d/hints", root, nodeNum),
                                  String.format("%s/node%d/cdc", root, nodeNum),
                                  token);
    }

    public InstanceConfig forVersion(Versions.Major major)
    {
        switch (major)
        {
            case v4: return this;
            default: return new InstanceConfig(this)
                            .set("seed_provider", new ParameterizedClass(SimpleSeedProvider.class.getName(),
                                                                         Collections.singletonMap("seeds", "127.0.0.1")));
        }
    }

    public String toString()
    {
        return params.toString();
    }
}
