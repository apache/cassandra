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

package org.apache.cassandra.distributed;

import java.io.File;
import java.io.Serializable;
import java.util.UUID;

public class InstanceConfig implements Serializable
{
    public final int num;
    public final UUID hostId =java.util.UUID.randomUUID();
    public final String partitioner = "org.apache.cassandra.dht.Murmur3Partitioner";
    public final String broadcast_address;
    public final String listen_address;
    public final String broadcast_rpc_address;
    public final String rpc_address;
    public final String saved_caches_directory;
    public final String[] data_file_directories;
    public final String commitlog_directory;
    public final String hints_directory;
    public final String cdc_directory;
    public final int concurrent_writes = 2;
    public final int concurrent_counter_writes = 2;
    public final int concurrent_materialized_view_writes = 2;
    public final int concurrent_reads = 2;
    public final int memtable_flush_writers = 1;
    public final int concurrent_compactors = 1;
    public final int memtable_heap_space_in_mb = 10;
    public final String initial_token;

    private InstanceConfig(int num,
                           String broadcast_address,
                           String listen_address,
                           String broadcast_rpc_address,
                           String rpc_address,
                           String saved_caches_directory,
                           String[] data_file_directories,
                           String commitlog_directory,
                           String hints_directory,
                           String cdc_directory,
                           String initial_token)
    {
        this.num = num;
        this.broadcast_address = broadcast_address;
        this.listen_address = listen_address;
        this.broadcast_rpc_address = broadcast_rpc_address;
        this.rpc_address = rpc_address;
        this.saved_caches_directory = saved_caches_directory;
        this.data_file_directories = data_file_directories;
        this.commitlog_directory = commitlog_directory;
        this.hints_directory = hints_directory;
        this.cdc_directory = cdc_directory;
        this.initial_token = initial_token;
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
}
