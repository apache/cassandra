/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.FBUtilities;

public class LocalStrategy extends AbstractReplicationStrategy
{
    public LocalStrategy(String table, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions)
    {
        super(table, tokenMetadata, snitch, configOptions);
    }

    public List<InetAddress> calculateNaturalEndpoints(Token token, TokenMetadata metadata)
    {
        return Arrays.asList(FBUtilities.getBroadcastAddress());
    }

    public int getReplicationFactor()
    {
        return 1;
    }

    public void validateOptions() throws ConfigurationException
    {
        // LocalStrategy doesn't expect any options.
    }
}
