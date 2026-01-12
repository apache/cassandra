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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.service.DataResurrectionCheck;
import org.apache.cassandra.service.FileSystemOwnershipCheck;
import org.apache.cassandra.service.StartupCheck;
import org.apache.cassandra.service.StartupChecks;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.config.StartupChecksConfiguration.ENABLED_PROPERTY;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StartupChecksConfigurationTest
{
    @Test
    public void testStartupOptionsConfigApplication()
    {
        Map<String, Map<String, Object>> config = new HashMap<>()
        {{
            put("check_filesystem_ownership", new HashMap<>()
            {{
                put(ENABLED_PROPERTY, true);
                put("key", "value");
            }});
        }};

        StartupChecks startupChecks = new StartupChecks().withDefaultTests().withTest(new FileSystemOwnershipCheck());
        StartupChecksConfiguration options = new StartupChecksConfiguration(startupChecks, config);

        assertTrue(Boolean.parseBoolean(options.getConfig("check_filesystem_ownership")
                                               .get(ENABLED_PROPERTY)
                                               .toString()));

        assertEquals("value", options.getConfig("check_filesystem_ownership").get("key"));
        options.set("check_filesystem_ownership", "key", "value2");
        assertEquals("value2", options.getConfig("check_filesystem_ownership").get("key"));

        assertTrue(options.isEnabled("check_filesystem_ownership"));
        options.disable("check_filesystem_ownership");
        assertFalse(options.isEnabled("check_filesystem_ownership"));
        assertTrue(options.isDisabled("check_filesystem_ownership"));
    }

    @Test
    public void testNoOptions()
    {

        StartupChecks startupChecks = new StartupChecks().withDefaultTests();

        StartupChecksConfiguration options = new StartupChecksConfiguration(new StartupChecks().withDefaultTests(), new HashMap<>());

        for (StartupCheck check : startupChecks.getChecks())
        {
            if (!check.isConfigurable())
                assertTrue(options.isEnabled(check.name()));
        }

        // disabling does not do anything on non-configurable check

        Optional<StartupCheck> nonConfigurableCheck = startupChecks.getChecks().stream().filter(check -> !check.isConfigurable()).findFirst();

        Assert.assertTrue(nonConfigurableCheck.isPresent());

        String checkName = nonConfigurableCheck.get().name();

        options.disable(checkName);

        assertTrue(options.isEnabled(checkName));

        options.set(checkName, "key", "value");
        // we can not put anything into non-configurable check
        Map<String, Object> nonConfigurableCheckConfig = options.getConfig(checkName);
        assertNotNull(nonConfigurableCheckConfig);
        assertFalse(nonConfigurableCheckConfig.containsKey("key"));
    }

    @Test
    public void testEmptyDisabledValues()
    {
        Map<String, Map<String, Object>> emptyConfig = new HashMap<>()
        {{
            put("check_filesystem_ownership", new HashMap<>());
        }};

        Map<String, Map<String, Object>> emptyEnabledConfig = new HashMap<>()
        {{
            put("check_filesystem_ownership", new HashMap<>()
            {{
                put(ENABLED_PROPERTY, null);
            }});
        }};

        // empty enabled property or enabled property with null value are still counted as disabled

        StartupChecks startupChecks = new StartupChecks().withDefaultTests().withTest(new FileSystemOwnershipCheck());
        StartupChecksConfiguration options1 = new StartupChecksConfiguration(startupChecks, emptyConfig);
        assertTrue(options1.isDisabled("check_filesystem_ownership"));

        StartupChecksConfiguration options2 = new StartupChecksConfiguration(startupChecks, emptyEnabledConfig);
        assertTrue(options2.isDisabled("check_filesystem_ownership"));
    }

    @Test
    public void testChecksDisabledByDefaultAreNotEnabled()
    {
        Map<String, Map<String, Object>> emptyConfig = new HashMap<>();
        StartupChecksConfiguration options = new StartupChecksConfiguration(new StartupChecks().withDefaultTests(), emptyConfig);
        assertTrue(options.isDisabled("check_filesystem_ownership"));
    }

    @Test
    public void testExcludedKeyspacesInDataResurrectionCheckOptions()
    {
        Map<String, Object> config = new HashMap<>()
        {{
            put("excluded_keyspaces", "ks1,ks2,ks3");
        }};
        DataResurrectionCheck check = new DataResurrectionCheck();
        check.getExcludedKeyspaces(config);

        Set<String> excludedKeyspaces = check.getExcludedKeyspaces(config);
        assertEquals(3, excludedKeyspaces.size());
        assertTrue(excludedKeyspaces.contains("ks1"));
        assertTrue(excludedKeyspaces.contains("ks2"));
        assertTrue(excludedKeyspaces.contains("ks3"));
    }

    @Test
    public void testExcludedTablesInDataResurrectionCheckOptions()
    {
        for (String input : new String[]{
        "ks1.tb1,ks1.tb2,ks3.tb3",
        " ks1 . tb1,  ks1 .tb2  ,ks3 .tb3  "
        })
        {
            Map<String, Object> config = new HashMap<>()
            {{
                put("excluded_tables", input);
            }};

            DataResurrectionCheck check = new DataResurrectionCheck();
            Set<Pair<String, String>> excludedTables = check.getExcludedTables(config);
            assertEquals(3, excludedTables.size());
            assertTrue(excludedTables.contains(Pair.create("ks1", "tb1")));
            assertTrue(excludedTables.contains(Pair.create("ks1", "tb2")));
            assertTrue(excludedTables.contains(Pair.create("ks3", "tb3")));
        }
    }

    @Test
    public void testNonConfigurableCheckIsNotConfigurable()
    {
        StartupChecks startupChecks = new StartupChecks().withDefaultTests();
        Optional<StartupCheck> maybeNotConfigurableCheck = startupChecks.getChecks().stream().filter(check -> !check.isConfigurable()).findFirst();
        Assert.assertTrue(maybeNotConfigurableCheck.isPresent());

        StartupCheck check = maybeNotConfigurableCheck.get();

        Map<String, Map<String, Object>> config = new HashMap<>()
        {
            {
                put(check.name(), new HashMap<>()
                {{
                    put(ENABLED_PROPERTY, false);
                    put("key", "value");
                }});
            }
        };

        assertThatThrownBy(() -> new StartupChecksConfiguration(startupChecks, config))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(String.format("There are configuration entries for startup checks which are not configurable: [%s]", check.name()));
    }

    @Test
    public void testNotExistingCheckYieldsInvalidState()
    {
        StartupChecks startupChecks = new StartupChecks().withDefaultTests();
        Map<String, Map<String, Object>> config = new HashMap<>()
        {
            {
                put("jemalloc", new HashMap<>()
                {{
                    put(ENABLED_PROPERTY, true);
                    put("key", "value");
                }});
                put("check_data_resurrection", new HashMap<>()
                {{
                    put(ENABLED_PROPERTY, true);
                    put("key2", "value2");
                }});
            }
        };

        assertThatThrownBy(() -> new StartupChecksConfiguration(startupChecks, config))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("There are configuration entries for startup checks which are not configurable: [jemalloc]");
    }
}
