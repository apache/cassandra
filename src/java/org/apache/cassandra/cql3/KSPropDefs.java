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
package org.apache.cassandra.cql3;

import java.util.*;

import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.thrift.InvalidRequestException;

public class KSPropDefs extends PropertyDefinitions
{
    public static final String KW_DURABLE_WRITES = "durable_writes";

    public static final String KW_REPLICATION_STRATEGY = "strategy_class";

    public static final Set<String> keywords = new HashSet<String>();
    public static final Set<String> obsoleteKeywords = new HashSet<String>();

    static
    {
        keywords.add(KW_DURABLE_WRITES);
        keywords.add(KW_REPLICATION_STRATEGY);
    }

    private String strategyClass;
    private final Map<String, String> strategyOptions = new HashMap<String, String>();

    public void validate() throws ConfigurationException, InvalidRequestException
    {
        validate(keywords, obsoleteKeywords);

        if (!properties.containsKey("strategy_class"))
            throw new InvalidRequestException("missing required argument \"strategy_class\"");
        strategyClass = properties.get("strategy_class");
    }

    @Override
    public void addProperty(String name, String value) throws InvalidRequestException
    {
        // optional
        if (name.contains(":") && name.startsWith("strategy_options"))
            strategyOptions.put(name.split(":")[1], value);
        else
            super.addProperty(name, value);
    }

    public String getReplicationStrategyClass()
    {
        return strategyClass;
    }

    public Map<String, String> getReplicationOptions()
    {
        return strategyOptions;
    }

    public KSMetaData asKSMetadata(String ksName) throws InvalidRequestException, ConfigurationException
    {
        return KSMetaData.newKeyspace(ksName, getReplicationStrategyClass(), getReplicationOptions(), getBoolean(KW_DURABLE_WRITES, true));
    }

    public KSMetaData asKSMetadataUpdate(KSMetaData old) throws InvalidRequestException, ConfigurationException
    {
        String sClass = strategyClass;
        Map<String, String> sOptions = getReplicationOptions();
        if (sClass == null)
        {
            sClass = old.strategyClass.getName();
            sOptions = old.strategyOptions;
        }
        return KSMetaData.newKeyspace(old.name, sClass, sOptions, getBoolean(KW_DURABLE_WRITES, old.durableWrites));
    }
}
