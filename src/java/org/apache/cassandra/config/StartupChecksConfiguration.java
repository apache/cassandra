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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.service.StartupCheck;
import org.apache.cassandra.service.StartupChecks;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

public class StartupChecksConfiguration
{
    public static final String ENABLED_PROPERTY = "enabled";

    private final Map<String, Map<String, Object>> options = new HashMap<>();
    private final StartupChecks startupChecks;

    public StartupChecksConfiguration(StartupChecks startupChecks, Map<String, Map<String, Object>> options)
    {
        this.options.putAll(new HashMap<>(options));
        this.startupChecks = startupChecks;

        apply();
    }

    @VisibleForTesting
    public StartupCheck getCheck(String name)
    {
        return startupChecks.getCheck(name);
    }

    private StartupCheck getConfigurableCheck(String name)
    {
        StartupCheck check = startupChecks.getCheck(name);
        if (check == null || !check.isConfigurable())
            return null;
        else
            return check;
    }

    public void set(String name, String key, Object value)
    {
        StartupCheck check = getConfigurableCheck(name);
        if (check == null)
            return;

        Map<String, Object> checkConfiguration = options.get(name);
        if (checkConfiguration == null)
            return;

        checkConfiguration.put(key, value);
    }

    public void enable(String name)
    {
        set(name, ENABLED_PROPERTY, TRUE);
    }

    public void disable(String name)
    {
        set(name, ENABLED_PROPERTY, FALSE);
    }

    public boolean isEnabled(String name)
    {
        Map<String, Object> config = getConfig(name);
        if (config == null)
            return false;

        Object enabledBoolean = config.get(ENABLED_PROPERTY);
        if (enabledBoolean == null)
            return false;

        return Boolean.parseBoolean(enabledBoolean.toString());
    }

    public boolean isDisabled(String name)
    {
        return !isEnabled(name);
    }

    public Map<String, Object> getConfig(String name)
    {
        return options.get(name);
    }

    private void apply()
    {
        List<String> notExistingCheckNames = new ArrayList<>();
        List<String> notConfigurableCheckNames = new ArrayList<>();

        for (Map.Entry<String, Map<String, Object>> userConfigEntry : options.entrySet())
        {
            String key = userConfigEntry.getKey();
            StartupCheck check = startupChecks.getCheck(key);
            if (check == null)
                notExistingCheckNames.add(key);
            else if (!check.isConfigurable())
                notConfigurableCheckNames.add(key);
        }

        if (!notExistingCheckNames.isEmpty())
            throw new IllegalStateException("There are configuration entries for startup checks which do not exist: " + notExistingCheckNames);
        if (!notConfigurableCheckNames.isEmpty())
            throw new IllegalStateException("There are configuration entries for startup checks which are not configurable: " + notConfigurableCheckNames);

        for (StartupCheck check : startupChecks.getChecks())
        {
            String startupCheckName = check.name();
            Map<String, Object> configMap = this.options.computeIfAbsent(startupCheckName, k -> new HashMap<>());
            if (configMap.containsKey(ENABLED_PROPERTY))
                configMap.putIfAbsent(ENABLED_PROPERTY, FALSE);
            else if (check.isDisabledByDefault())
                configMap.put(ENABLED_PROPERTY, FALSE);
            else
                configMap.put(ENABLED_PROPERTY, TRUE);
        }
    }

    public void verify() throws StartupException
    {
        assert startupChecks != null;
        startupChecks.verify(this);
    }
}
