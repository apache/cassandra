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
package org.apache.cassandra.cql;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.exceptions.InvalidRequestException;

/** A <code>CREATE KEYSPACE</code> statement parsed from a CQL query. */
public class CreateKeyspaceStatement
{
    private final String name;
    private final Map<String, String> attrs;
    private String strategyClass;
    private final Map<String, String> strategyOptions = new HashMap<String, String>();

    /**
     * Creates a new <code>CreateKeyspaceStatement</code> instance for a given
     * keyspace name and keyword arguments.
     *
     * @param name the name of the keyspace to create
     * @param attrs map of the raw keyword arguments that followed the <code>WITH</code> keyword.
     */
    public CreateKeyspaceStatement(String name, Map<String, String> attrs)
    {
        this.name = name;
        this.attrs = attrs;
    }

    /**
     * The <code>CqlParser</code> only goes as far as extracting the keyword arguments
     * from these statements, so this method is responsible for processing and
     * validating, and must be called prior to access.
     *
     * @throws InvalidRequestException if arguments are missing or unacceptable
     */
    public void validate() throws InvalidRequestException
    {
        // required
        if (!attrs.containsKey("strategy_class"))
            throw new InvalidRequestException("missing required argument \"strategy_class\"");
        strategyClass = attrs.get("strategy_class");

        // optional
        for (String key : attrs.keySet())
            if ((key.contains(":")) && (key.startsWith("strategy_options")))
                strategyOptions.put(key.split(":")[1], attrs.get(key));
    }

    public String getName()
    {
        return name;
    }

    public String getStrategyClass()
    {
        return strategyClass;
    }

    public Map<String, String> getStrategyOptions()
    {
        return strategyOptions;
    }
}
