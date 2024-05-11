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
package org.apache.cassandra.schema;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;

import static java.lang.String.format;

public final class AutoRepairParams
{
    public enum Option
    {
        ENABLED;

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    public static final Map<AutoRepairConfig.RepairType, Map<String, String>> DEFAULT_OPTIONS =
    ImmutableMap.of(AutoRepairConfig.RepairType.full, ImmutableMap.of(Option.ENABLED.toString(), Boolean.toString(true)),
                    AutoRepairConfig.RepairType.incremental, ImmutableMap.of(Option.ENABLED.toString(), Boolean.toString(true)));

    public final AutoRepairConfig.RepairType type;

    private Map<AutoRepairConfig.RepairType, Map<String, String>> options = DEFAULT_OPTIONS;

    AutoRepairParams(AutoRepairConfig.RepairType type)
    {
        this.type = type;
    }

    public static AutoRepairParams create(AutoRepairConfig.RepairType repairType, Map<String, String> options)
    {
        Map<AutoRepairConfig.RepairType, Map<String, String>> optionsMap = new HashMap<>();
        for (Map.Entry<AutoRepairConfig.RepairType, Map<String, String>> entry : DEFAULT_OPTIONS.entrySet())
        {
            optionsMap.put(entry.getKey(), new HashMap<>(entry.getValue()));
        }
        for (Map.Entry<String, String> entry : options.entrySet())
        {
            if (!Option.ENABLED.toString().equals(entry.getKey().toLowerCase()))
            {
                throw new ConfigurationException(format("Unknown property '%s'", entry.getKey()));
            }
            optionsMap.get(repairType).put(entry.getKey(), entry.getValue());
        }
        AutoRepairParams repairParams = new AutoRepairParams(repairType);
        repairParams.options = optionsMap;
        return repairParams;
    }

    public boolean repairEnabled()
    {
        String enabled = options.get(type).get(Option.ENABLED.toString());
        return enabled == null
               ? Boolean.parseBoolean(DEFAULT_OPTIONS.get(type).get(Option.ENABLED.toString()))
               : Boolean.parseBoolean(enabled);
    }

    public void validate()
    {
        String enabled = options.get(type).get(Option.ENABLED.toString());
        if (enabled != null && !isValidBoolean(enabled))
        {
            throw new ConfigurationException(format("Invalid value %s for '%s' repair sub-option - must be a boolean",
                                                    enabled,
                                                    Option.ENABLED));
        }
    }

    public static boolean isValidBoolean(String value)
    {
        return StringUtils.equalsIgnoreCase(value, "true") || StringUtils.equalsIgnoreCase(value, "false");
    }

    public Map<String, String> options()
    {
        return options.get(type);
    }

    public static AutoRepairParams fromMap(AutoRepairConfig.RepairType repairType, Map<String, String> map)
    {
        return create(repairType, map);
    }

    public Map<String, String> asMap()
    {
        return options.get(type);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("options", options)
                          .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof AutoRepairParams))
            return false;

        AutoRepairParams cp = (AutoRepairParams) o;

        return options.equals(cp.options);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(options);
    }
}
