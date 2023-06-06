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

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.yaml.snakeyaml.introspector.Property;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ValidatedPropertyLoaderTest
{
    private static final Loader LOADER = Properties.validatedPropertyLoader();
    private static volatile String lastSeenPropertyNotInYaml = null;

    @After
    public void reset()
    {
        lastSeenPropertyNotInYaml = null;
    }

    @Test
    public void testListenablePropertyMethodPattern() throws Exception
    {
        Property property = LOADER.getProperties(TestConfig.class).get("test_property");

        assertThatThrownBy(() -> property.set(new TestConfig(), 42))
        .hasRootCauseInstanceOf(ConfigurationException.class)
        .hasRootCauseMessage("Value for the property 'test_property' must not be equal to 42");

        assertThatThrownBy(() -> property.set(new TestConfig(), 41))
        .hasRootCauseInstanceOf(ConfigurationException.class)
        .hasRootCauseMessage("TestConfig instance TestConfig{}, the value for the property 'test_property' must not be equal to 41");

        assertThatThrownBy(() -> property.set(new TestConfig(), 40))
        .hasRootCauseInstanceOf(ConfigurationException.class)
        .hasRootCauseMessage("Value must not be equal to 40");
    }

    @Test
    public void testListenablePropertyExists() throws Exception
    {
        assertThat(LOADER.getProperties(TestConfig.class).get("test_property")).isNotNull();
        assertThat(LOADER.getProperties(TestConfig.class).get("test_property")).isInstanceOf(ListenableProperty.class);
    }

    @Test
    public void testListenablePropertyNotExists() throws Exception
    {
        assertThat(LOADER.getProperties(TestConfig.class).get("test_property_not_exists")).isNull();
    }

    @Test
    public void testListenablePropertyMigthtBeSet() throws Exception
    {
        TestConfig conf = new TestConfig();
        LOADER.getProperties(TestConfig.class).get("test_property").set(conf, 39);
        assertThat(conf.test_property).isEqualTo(39);
    }

    @Test
    public void testListenablePropertyHandler() throws Exception
    {
        TestConfig conf = new TestConfig();
        Property property = LOADER.getProperties(TestConfig.class).get("test_string_property");
        assertThatThrownBy(() -> property.set(conf, "test"))
        .hasRootCauseInstanceOf(ConfigurationException.class)
        .hasRootCauseMessage("Value must be null");

        property.set(conf, null);
        assertThat(property.get(conf)).isEqualTo("test_string_property_value");
        assertThat(conf.test_string_property).isEqualTo("test_string_property_value");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testListenablePropertyCustomListener() throws Exception
    {
        Property property = LOADER.getProperties(TestConfig.class).get("test_property");
        assertThat(LOADER.getProperties(TestConfig.class).get("test_property")).isInstanceOf(ListenableProperty.class);

        ListenableProperty<TestConfig, Integer> listenableProperty = (ListenableProperty<TestConfig, Integer>) property;
        listenableProperty.addListener(new ListenableProperty.Listener<TestConfig, Integer>()
        {
            @Override
            public Integer before(TestConfig source, String name, Integer oldValue, Integer newValue)
            {
                if (newValue > 100)
                    throw new ConfigurationException("Value must not be greater than 100");
                return newValue;
            }
        });

        TestConfig conf = new TestConfig();
        assertThatThrownBy(() -> property.set(conf, 200))
        .isInstanceOf(ConfigurationException.class)
        .hasMessage("Value must not be greater than 100");

        LOADER.getProperties(TestConfig.class).get("test_property").set(conf, 10);
        assertThat(conf.test_property).isEqualTo(10);
    }

    @Test
    public void testLoadCustomConfig() throws Exception
    {
        TestConfig conf = YamlConfigurationLoaderTest.load("test/conf/test-configuraiton.yaml", TestConfig.class, TestConfig::new);

        assertThat(conf.test_property).isEqualTo(10);
        assertThat(conf.test_string_property).isEqualTo("test_string_property_value");
        assertThat(conf.nested_test_config.good_string_to_go_with).isEqualTo("less 10");
        assertThat(lastSeenPropertyNotInYaml).isEqualTo("test_property_not_in_yaml");
    }

    /**
     * This method is used to validate the test_property_not_in_yaml field in the TestConfig class.
     */
    public static void validatePropertyNotInYaml(String value)
    {
        lastSeenPropertyNotInYaml = value;
    }

    public static class TestConfig
    {
        @ValidatedBy(useClass = "org.apache.cassandra.config.ValidatedPropertyLoaderTest$TestConfig", useClassMethod = "validateTestProperty1Arguments")
        @ValidatedBy(useClass = "org.apache.cassandra.config.ValidatedPropertyLoaderTest$TestConfig", useClassMethod = "validateTestProperty2Arguments")
        @ValidatedBy(useClass = "org.apache.cassandra.config.ValidatedPropertyLoaderTest$TestConfig", useClassMethod = "validateTestProperty3Arguments")
        public Integer test_property = 0;

        @ValidatedBy(useClass = "org.apache.cassandra.config.ValidatedPropertyLoaderTest$TestConfig", useClassMethod = "handleTestStringProperty")
        public String test_string_property = null;

        @ValidatedBy(useClass = "org.apache.cassandra.config.ValidatedPropertyLoaderTest", useClassMethod = "validatePropertyNotInYaml")
        public String test_property_not_in_yaml = "test_property_not_in_yaml";

        public NestedTestConfig nested_test_config = new NestedTestConfig();

        public static void validateTestProperty1Arguments(Integer value)
        {
            if (value == 40)
                throw new ConfigurationException("Value must not be equal to 40");
        }

        public static void validateTestProperty2Arguments(String name, Integer value)
        {
            if (value == 42)
                throw new ConfigurationException("Value for the property '" + name + "' must not be equal to 42");
        }

        public static void validateTestProperty3Arguments(TestConfig conf, String name, Integer value)
        {
            if (value == 41)
                throw new ConfigurationException("TestConfig instance " + conf + ", the value for the property '" + name + "' must not be equal to 41");
        }

        public static String handleTestStringProperty(String value)
        {
            if (value == null)
                return "test_string_property_value";
            throw new ConfigurationException("Value must be null");
        }

        @Override
        public String toString()
        {
            return "TestConfig{}";
        }
    }

    public static class NestedTestConfig
    {
        @ValidatedBy(useClass = "org.apache.cassandra.config.ValidatedPropertyLoaderTest$NestedTestConfig", useClassMethod = "validateTestProperty1Arguments")
        public String good_string_to_go_with = "default value";

        public static void validateTestProperty1Arguments(String value)
        {
            if (value.length() > 10)
                throw new ConfigurationException("String value must be greater than 10 characters, but was " + value.length() + " characters long");
        }
    }
}