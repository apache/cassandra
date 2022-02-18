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

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.service.StartupChecks.StartupCheckType;

import static org.apache.cassandra.config.StartupChecksOptions.ENABLED_PROPERTY;
import static org.apache.cassandra.service.StartupChecks.StartupCheckType.filesystem_ownership;
import static org.apache.cassandra.service.StartupChecks.StartupCheckType.non_configurable_check;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StartupCheckOptionsTest
{
    @Test
    public void testStartupOptionsConfigApplication()
    {
        Map<StartupCheckType, Map<String, Object>> config = new EnumMap<StartupCheckType, Map<String, Object>>(StartupCheckType.class) {{
            put(filesystem_ownership, new HashMap<String, Object>() {{
                put(ENABLED_PROPERTY, true);
                put("key", "value");
            }});
        }};

        StartupChecksOptions options = new StartupChecksOptions(config);

        assertTrue(Boolean.parseBoolean(options.getConfig(filesystem_ownership)
                                               .get(ENABLED_PROPERTY)
                                               .toString()));

        assertEquals("value", options.getConfig(filesystem_ownership).get("key"));
        options.set(filesystem_ownership, "key", "value2");
        assertEquals("value2", options.getConfig(filesystem_ownership).get("key"));

        assertTrue(options.isEnabled(filesystem_ownership));
        options.disable(filesystem_ownership);
        assertFalse(options.isEnabled(filesystem_ownership));
        assertTrue(options.isDisabled(filesystem_ownership));
    }

    @Test
    public void testNoOptions()
    {
        StartupChecksOptions options = new StartupChecksOptions();

        assertTrue(options.isEnabled(non_configurable_check));

        // disabling does not to anything on non-configurable check
        options.disable(non_configurable_check);
        assertTrue(options.isEnabled(non_configurable_check));

        options.set(non_configurable_check, "key", "value");

        // we can not put anything into non-configurable check
        assertFalse(options.getConfig(non_configurable_check).containsKey("key"));
    }

    @Test
    public void testEmptyDisabledValues()
    {
        Map<StartupCheckType, Map<String, Object>> emptyConfig = new EnumMap<StartupCheckType, Map<String, Object>>(StartupCheckType.class) {{
            put(filesystem_ownership, new HashMap<>());
        }};

        Map<StartupCheckType, Map<String, Object>> emptyEnabledConfig = new EnumMap<StartupCheckType, Map<String, Object>>(StartupCheckType.class) {{
            put(filesystem_ownership, new HashMap<String, Object>() {{
                put(ENABLED_PROPERTY, null);
            }});
        }};

        // empty enabled property or enabled property with null value are still counted as enabled

        StartupChecksOptions options1 = new StartupChecksOptions(emptyConfig);
        assertTrue(options1.isDisabled(filesystem_ownership));

        StartupChecksOptions options2 = new StartupChecksOptions(emptyEnabledConfig);
        assertTrue(options2.isDisabled(filesystem_ownership));
    }

    @Test
    public void testChecksDisabledByDefaultAreNotEnabled()
    {
        Map<StartupCheckType, Map<String, Object>> emptyConfig = new EnumMap<>(StartupCheckType.class);
        StartupChecksOptions options = new StartupChecksOptions(emptyConfig);
        assertTrue(options.isDisabled(filesystem_ownership));
    }
}
