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
package org.apache.cassandra.concurrent;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

public enum Stage
{
    READ,
    MUTATION,
    COUNTER_MUTATION,
    VIEW_MUTATION,
    GOSSIP,
    REQUEST_RESPONSE,
    ANTI_ENTROPY,
    MIGRATION,
    MISC,
    TRACING,
    INTERNAL_RESPONSE,
    IMMEDIATE;

    public String getJmxType()
    {
        switch (this)
        {
            case ANTI_ENTROPY:
            case GOSSIP:
            case MIGRATION:
            case MISC:
            case TRACING:
            case INTERNAL_RESPONSE:
            case IMMEDIATE:
                return "internal";
            case MUTATION:
            case COUNTER_MUTATION:
            case VIEW_MUTATION:
            case READ:
            case REQUEST_RESPONSE:
                return "request";
            default:
                throw new AssertionError("Unknown stage " + this);
        }
    }

    public String getJmxName()
    {
        String name = "";
        for (String word : toString().split("_"))
        {
            name += word.substring(0, 1) + word.substring(1).toLowerCase();
        }
        return name + "Stage";
    }

    private static String normalizeName(String stageName)
    {
        // Handle discrepancy between JMX names and actual pool names
        String upperStageName = stageName.toUpperCase();
        if (upperStageName.endsWith("STAGE"))
        {
            upperStageName = upperStageName.substring(0, stageName.length() - 5);
        }
        return upperStageName;
    }

    private static Map<String,Stage> nameMap = Arrays.stream(values())
                                                     .collect(toMap(s -> Stage.normalizeName(s.getJmxName()),
                                                                    s -> s));

    public static Stage fromPoolName(String stageName)
    {
        String upperStageName = normalizeName(stageName);

        Stage result = nameMap.get(upperStageName);
        if (result != null)
            return result;

        try
        {
            return valueOf(upperStageName);
        }
        catch (IllegalArgumentException e)
        {
            switch(upperStageName) // Handle discrepancy between configuration file and stage names
            {
                case "CONCURRENT_READS":
                    return READ;
                case "CONCURRENT_WRITERS":
                    return MUTATION;
                case "CONCURRENT_COUNTER_WRITES":
                    return COUNTER_MUTATION;
                case "CONCURRENT_MATERIALIZED_VIEW_WRITES":
                    return VIEW_MUTATION;
                default:
                    throw new IllegalStateException("Must be one of " + Arrays.stream(values())
                                                                              .map(Enum::toString)
                                                                              .collect(Collectors.joining(",")));
            }
        }
    }
}
