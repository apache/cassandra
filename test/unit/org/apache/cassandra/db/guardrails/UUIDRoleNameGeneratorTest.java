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

package org.apache.cassandra.db.guardrails;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;

import static java.util.Map.of;
import static org.apache.cassandra.db.guardrails.UUIDRoleNameGenerator.DEFAULT_MINIMUM_NAME_SIZE;
import static org.apache.cassandra.db.guardrails.UUIDRoleNameGenerator.MAXIMUM_NAME_SIZE;
import static org.apache.cassandra.db.guardrails.UUIDRoleNameGenerator.MINIMUM_NAME_SIZE_CONFIG_OPTION;
import static org.apache.cassandra.db.guardrails.UUIDRoleNameGenerator.NAME_PREFIX_KEY;
import static org.apache.cassandra.db.guardrails.UUIDRoleNameGenerator.NAME_SIZE;
import static org.apache.cassandra.db.guardrails.UUIDRoleNameGenerator.NAME_SUFFIX_KEY;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class UUIDRoleNameGeneratorTest
{
    @Test
    public void testRoleNameGenerator()
    {
        assertGeneratedRoleNameSize(new UUIDRoleNameGenerator().generate(), MAXIMUM_NAME_SIZE);

        assertTrue(new UUIDRoleNameGenerator()
                   .generate(of(NAME_PREFIX_KEY, "this_is_prefix_"))
                   .startsWith("this_is_prefix_"));

        assertTrue(new UUIDRoleNameGenerator()
                   .generate(of(NAME_SUFFIX_KEY, "_this_is_suffix"))
                   .endsWith("_this_is_suffix"));

        String generatedPassword = new UUIDRoleNameGenerator().generate(of(NAME_PREFIX_KEY,
                                                                           "this_is_prefix_",
                                                                           NAME_SUFFIX_KEY,
                                                                           "_this_is_suffix"));

        assertTrue(generatedPassword.endsWith("_this_is_suffix"));
        assertTrue(generatedPassword.startsWith("this_is_prefix_"));

        assertGeneratedRoleNameSize(new UUIDRoleNameGenerator().generate(of(NAME_SIZE, 15)), 15);

        // more than 32 errors out

        assertThatThrownBy(() -> new UUIDRoleNameGenerator().generate(of(NAME_SIZE, 45)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Generator generates names of maximum length " + MAXIMUM_NAME_SIZE + ". You want to generate with length 45.");

        // assert edge cases
        assertGeneratedRoleNameSize(new UUIDRoleNameGenerator().generate(of(NAME_SIZE, MAXIMUM_NAME_SIZE)), MAXIMUM_NAME_SIZE);
        assertGeneratedRoleNameSize(new UUIDRoleNameGenerator().generate(of(NAME_SIZE, DEFAULT_MINIMUM_NAME_SIZE)), DEFAULT_MINIMUM_NAME_SIZE);

        // when configured minimum size of 15, less than 15 errors out
        CustomGuardrailConfig config = new CustomGuardrailConfig();
        config.put(MINIMUM_NAME_SIZE_CONFIG_OPTION, 15);
        assertThatThrownBy(() -> new UUIDRoleNameGenerator(config).generate(of(NAME_SIZE, 13)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Value of name_size parameter has to be at least 15.");

        // same as above but try to configure edge
        CustomGuardrailConfig config2 = new CustomGuardrailConfig();
        config2.put(MINIMUM_NAME_SIZE_CONFIG_OPTION, 15);
        assertGeneratedRoleNameSize(new UUIDRoleNameGenerator(config2).generate(of(NAME_SIZE, 15)), 15);

        // configuring minimum name size less than 10 errors out
        CustomGuardrailConfig config3 = new CustomGuardrailConfig();
        config3.put(MINIMUM_NAME_SIZE_CONFIG_OPTION, 8);
        assertThatThrownBy(() -> new UUIDRoleNameGenerator(config3))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining(MINIMUM_NAME_SIZE_CONFIG_OPTION + " has to be at least " + DEFAULT_MINIMUM_NAME_SIZE + " and at most " + MAXIMUM_NAME_SIZE);

        CustomGuardrailConfig config4 = new CustomGuardrailConfig();
        config4.put(MINIMUM_NAME_SIZE_CONFIG_OPTION, 55);
        assertThatThrownBy(() -> new UUIDRoleNameGenerator(config4))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining(MINIMUM_NAME_SIZE_CONFIG_OPTION + " has to be at least " + DEFAULT_MINIMUM_NAME_SIZE + " and at most " + MAXIMUM_NAME_SIZE);
    }

    @Test
    public void testOptions()
    {
        // integer
        assertGeneratedRoleNameSize(new UUIDRoleNameGenerator().generate(of(NAME_SIZE, 15)), 15);

        // length as string
        assertGeneratedRoleNameSize(new UUIDRoleNameGenerator().generate(of(NAME_SIZE, "15")), 15);

        // length as float, will be converted to string
        assertGeneratedRoleNameSize(new UUIDRoleNameGenerator().generate(of(NAME_SIZE, 15f)), 15);

        // invalid number
        assertThatThrownBy(() -> new UUIDRoleNameGenerator().generate(of(NAME_SIZE, "invalid number")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Value 'invalid number' can't be converted to integer.");

        // negative number as string
        assertThatThrownBy(() -> new UUIDRoleNameGenerator().generate(of(NAME_SIZE, "-1")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Value of name_size parameter has to be at least 10.");

        // negative number as integer
        assertThatThrownBy(() -> new UUIDRoleNameGenerator().generate(of(NAME_SIZE, -1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Value of name_size parameter has to be at least 10.");

        // name_size has to be string or integer
        assertThatThrownBy(() -> new UUIDRoleNameGenerator().generate(Map.of(NAME_SIZE, new Object())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported object passed to resolve " + NAME_SIZE + ": " + Object.class.getName());

        // null name_size
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(NAME_SIZE, null);
        assertThatThrownBy(() -> new UUIDRoleNameGenerator().generate(configMap))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Value of name_size has to be strictly positive integer.");

        // null prefix or suffix
        Map<String, Object> configMap2 = new HashMap<>();
        configMap2.put(NAME_PREFIX_KEY, null);
        assertThatThrownBy(() -> new UUIDRoleNameGenerator().generate(configMap2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Value of " + NAME_PREFIX_KEY + " cannot be null.");

        Map<String, Object> configMap3 = new HashMap<>();
        configMap3.put(NAME_SUFFIX_KEY, null);
        assertThatThrownBy(() -> new UUIDRoleNameGenerator().generate(configMap3))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Value of " + NAME_SUFFIX_KEY + " cannot be null.");

        // suffix and prefix has to be a string
        assertThatThrownBy(() -> new UUIDRoleNameGenerator().generate(Map.of(NAME_SUFFIX_KEY, 1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Value of " + NAME_SUFFIX_KEY + " is not a string.");

        assertThatThrownBy(() -> new UUIDRoleNameGenerator().generate(Map.of(NAME_PREFIX_KEY, 1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Value of " + NAME_PREFIX_KEY + " is not a string.");
    }

    @Test
    public void testGeneratedRoleNameAlwaysStartsWithALetter()
    {
        UUIDRoleNameGenerator generator = new UUIDRoleNameGenerator();

        // a thousand is just enough to see that it "always" starts with a letter
        for (int i = 0; i < 1000; i++)
        {
            String generatedName = generator.generate();
            assertNotNull(generatedName);

            char firstChar = generatedName.toCharArray()[0];
            boolean startsWithLetter = false;
            for (int j = 0; j < UUIDRoleNameGenerator.FIRST_CHARS.length; j++)
            {
                if (UUIDRoleNameGenerator.FIRST_CHARS[j] == firstChar)
                {
                    startsWithLetter = true;
                    break;
                }
            }

            if (!startsWithLetter)
                fail(generatedName + " does not start with a letter.");
        }
    }

    @Test
    public void testSupportedOptions()
    {
        Set<String> supportedOptionsKeys = new UUIDRoleNameGenerator().getSupportedOptionsKeys();
        assertTrue(supportedOptionsKeys.contains(NAME_PREFIX_KEY));
        assertTrue(supportedOptionsKeys.contains(NAME_SUFFIX_KEY));
        assertTrue(supportedOptionsKeys.contains(NAME_SIZE));
    }

    private void assertGeneratedRoleNameSize(String roleName, int size)
    {
        assertNotNull(roleName);
        assertEquals(size, roleName.length());
    }
}
