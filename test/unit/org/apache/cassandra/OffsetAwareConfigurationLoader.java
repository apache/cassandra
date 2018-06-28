/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;

import java.io.File;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;


public class OffsetAwareConfigurationLoader extends YamlConfigurationLoader
{

    static final String OFFSET_PROPERTY = "cassandra.test.offsetseed";
    int offset = 0;

    public OffsetAwareConfigurationLoader()
    {
        String offsetStr = System.getProperty(OFFSET_PROPERTY);

        if (offsetStr == null)
            throw new RuntimeException("offset property is not set: "+OFFSET_PROPERTY);

        offset = Integer.valueOf(offsetStr);

        assert offset >= 0;
    }

    @Override
    public Config loadConfig() throws ConfigurationException
    {
        Config config = super.loadConfig();

        String sep = File.pathSeparator;

        config.native_transport_port += offset;
        config.storage_port += offset;
        config.ssl_storage_port += offset;

        //Rewrite the seed ports string
        String[] hosts = config.seed_provider.parameters.get("seeds").split(",", -1);
        String rewrittenSeeds = Joiner.on(", ").join(Arrays.stream(hosts).map(host -> {
            StringBuilder sb = new StringBuilder();
            try
            {
                InetAddressAndPort address = InetAddressAndPort.getByName(host.trim());
                if (address.address instanceof Inet6Address)
                {
                     sb.append('[').append(address.address.getHostAddress()).append(']');
                }
                else
                {
                    sb.append(address.address.getHostAddress());
                }
                sb.append(':').append(address.port + offset);
                return sb.toString();
            }
            catch (UnknownHostException e)
            {
                throw new ConfigurationException("Error in OffsetAwareConfigurationLoader reworking seed list", e);
            }
        }).collect(Collectors.toList()));
        config.seed_provider.parameters.put("seeds", rewrittenSeeds);

        config.commitlog_directory += sep + offset;
        config.saved_caches_directory += sep + offset;
        config.hints_directory += sep + offset;

        config.cdc_raw_directory += sep + offset;

        for (int i = 0; i < config.data_file_directories.length; i++)
            config.data_file_directories[i] += sep + offset;

        return config;
    }
}
