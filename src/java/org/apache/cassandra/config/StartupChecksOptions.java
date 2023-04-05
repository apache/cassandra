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

package org.apache.cassandra.config;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.service.StartupChecks.StartupCheckType;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.apache.cassandra.service.StartupChecks.StartupCheckType.non_configurable_check;

public class StartupChecksOptions
{
    public static final String ENABLED_PROPERTY = "enabled";

    private final Map<StartupCheckType, Map<String, Object>> options = new EnumMap<>(StartupCheckType.class);

    public StartupChecksOptions()
    {
        this(Collections.emptyMap());
    }

    public StartupChecksOptions(final Map<StartupCheckType, Map<String, Object>> options)
    {
        this.options.putAll(options);
        apply();
    }

    public void set(final StartupCheckType startupCheckType, final String key, final Object value)
    {
        if (startupCheckType != non_configurable_check)
            options.get(startupCheckType).put(key, value);
    }

    public void enable(final StartupCheckType startupCheckType)
    {
        set(startupCheckType, ENABLED_PROPERTY, TRUE);
    }

    public void disable(final StartupCheckType startupCheckType)
    {
        if (startupCheckType != non_configurable_check)
            set(startupCheckType, ENABLED_PROPERTY, FALSE);
    }

    public boolean isEnabled(final StartupCheckType startupCheckType)
    {
        return Boolean.parseBoolean(options.get(startupCheckType).get(ENABLED_PROPERTY).toString());
    }

    public boolean isDisabled(final StartupCheckType startupCheckType)
    {
        return !isEnabled(startupCheckType);
    }

    public Map<String, Object> getConfig(final StartupCheckType startupCheckType)
    {
        return options.get(startupCheckType);
    }

    private void apply()
    {
        for (final StartupCheckType startupCheckType : StartupCheckType.values())
        {
            final Map<String, Object> configMap = options.computeIfAbsent(startupCheckType, k -> new HashMap<>());
            if (configMap.containsKey(ENABLED_PROPERTY))
                configMap.putIfAbsent(ENABLED_PROPERTY, FALSE);
            else if (startupCheckType.disabledByDefault)
                configMap.put(ENABLED_PROPERTY, FALSE);
            else
                configMap.put(ENABLED_PROPERTY, TRUE);
        }
        // clear if we put anything into it by accident & enable this check every time no matter what
        options.get(non_configurable_check).clear();
        options.get(non_configurable_check).put(ENABLED_PROPERTY, TRUE);
    }
}
