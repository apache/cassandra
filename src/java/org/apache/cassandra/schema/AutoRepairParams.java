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

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.utils.LocalizeString;

import static java.lang.String.format;
import static org.apache.cassandra.utils.LocalizeString.toLowerCaseLocalized;

/**
 * AutoRepair table parameters - used to define the auto-repair configuration for a table.
 */
public final class AutoRepairParams
{
    public enum Option
    {
        FULL_ENABLED,
        INCREMENTAL_ENABLED,
        PREVIEW_REPAIRED_ENABLED,
        PRIORITY;

        @Override
        public String toString()
        {
            return toLowerCaseLocalized(name());
        }
    }

    private final ImmutableMap<String, String> options;

    public static final Map<String, String> DEFAULT_OPTIONS = ImmutableMap.of(
    LocalizeString.toLowerCaseLocalized(Option.FULL_ENABLED.name()), Boolean.toString(true),
    LocalizeString.toLowerCaseLocalized(Option.INCREMENTAL_ENABLED.name()), Boolean.toString(true),
    LocalizeString.toLowerCaseLocalized(Option.PREVIEW_REPAIRED_ENABLED.name()), Boolean.toString(true),
    Option.PRIORITY.toString(), "0"
    );

    AutoRepairParams(Map<String, String> options)
    {
        this.options = ImmutableMap.copyOf(options);
    }

    public static final AutoRepairParams DEFAULT =
    new AutoRepairParams(DEFAULT_OPTIONS);

    public static AutoRepairParams create(Map<String, String> options)
    {
        Map<String, String> optionsMap = new TreeMap<>(DEFAULT_OPTIONS);
        if (options != null)
        {
            for (Map.Entry<String, String> entry : options.entrySet())
            {
                if (Arrays.stream(Option.values()).noneMatch(option -> option.toString().equalsIgnoreCase(entry.getKey())))
                {
                    throw new ConfigurationException(format("Unknown property '%s'", entry.getKey()));
                }
                optionsMap.put(entry.getKey(), entry.getValue());
            }
        }
        return new AutoRepairParams(optionsMap);
    }

    public boolean repairEnabled(AutoRepairConfig.RepairType type)
    {
        String option = LocalizeString.toLowerCaseLocalized(type.toString()) + "_enabled";
        String enabled = options.getOrDefault(option, DEFAULT_OPTIONS.get(option));
        return Boolean.parseBoolean(enabled);
    }

    public int priority()
    {
        String priority = options.getOrDefault(Option.PRIORITY.toString(), DEFAULT_OPTIONS.get(Option.PRIORITY.toString()));
        return Integer.parseInt(priority);
    }

    public void validate()
    {
        for (Option option : Option.values())
        {
            if (!options.containsKey(LocalizeString.toLowerCaseLocalized(option.toString())))
            {
                throw new ConfigurationException(format("Missing repair sub-option '%s'", option));
            }
        }
        if (options.get(LocalizeString.toLowerCaseLocalized(Option.FULL_ENABLED.toString())) != null && !isValidBoolean(options.get(LocalizeString.toLowerCaseLocalized(Option.FULL_ENABLED.toString()))))
        {
            throw new ConfigurationException(format("Invalid value %s for '%s' repair sub-option - must be a boolean",
                                                    options.get(LocalizeString.toLowerCaseLocalized(Option.FULL_ENABLED.toString())),
                                                    Option.FULL_ENABLED));
        }
        if (options.get(LocalizeString.toLowerCaseLocalized(Option.INCREMENTAL_ENABLED.toString())) != null && !isValidBoolean(options.get(LocalizeString.toLowerCaseLocalized(Option.INCREMENTAL_ENABLED.toString()))))
        {
            throw new ConfigurationException(format("Invalid value %s for '%s' repair sub-option - must be a boolean",
                                                    options.get(LocalizeString.toLowerCaseLocalized(Option.INCREMENTAL_ENABLED.toString())),
                                                    Option.INCREMENTAL_ENABLED));
        }
        if (options.get(LocalizeString.toLowerCaseLocalized(Option.PREVIEW_REPAIRED_ENABLED.toString())) != null && !isValidBoolean(options.get(LocalizeString.toLowerCaseLocalized(Option.PREVIEW_REPAIRED_ENABLED.toString()))))
        {
            throw new ConfigurationException(format("Invalid value %s for '%s' repair sub-option - must be a boolean",
                                                    options.get(LocalizeString.toLowerCaseLocalized(Option.PREVIEW_REPAIRED_ENABLED.toString())),
                                                    Option.PREVIEW_REPAIRED_ENABLED));
        }
        if (options.get(LocalizeString.toLowerCaseLocalized(Option.PRIORITY.toString())) != null && !isValidInt(options.get(LocalizeString.toLowerCaseLocalized(Option.PRIORITY.toString()))))
        {
            throw new ConfigurationException(format("Invalid value %s for '%s' repair sub-option - must be an integer",
                                                    options.get(LocalizeString.toLowerCaseLocalized(Option.PRIORITY.toString())),
                                                    Option.PRIORITY));
        }
    }

    public static boolean isValidBoolean(String value)
    {
        return StringUtils.equalsIgnoreCase(value, "true") || StringUtils.equalsIgnoreCase(value, "false");
    }

    public static boolean isValidInt(String value)
    {
        return StringUtils.isNumeric(value);
    }

    public Map<String, String> options()
    {
        return options;
    }

    public static AutoRepairParams fromMap(Map<String, String> map)
    {
        return create(map);
    }

    public Map<String, String> asMap()
    {
        return options;
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
