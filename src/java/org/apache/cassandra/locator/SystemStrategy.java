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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * A replication strategy that is uniform for all tokens and ignores TokenMetadata
 */
public abstract class SystemStrategy extends AbstractReplicationStrategy
{
    public SystemStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions)
    {
        super(keyspaceName, tokenMetadata, snitch, configOptions);
    }

    public void validateOptions() throws ConfigurationException
    {
    }

    @Override
    public void maybeWarnOnOptions()
    {
    }

    @Override
    public Collection<String> recognizedOptions()
    {
        // SystemStrategy doesn't expect any options.
        return Collections.emptySet();
    }
}