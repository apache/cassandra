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

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

import org.assertj.core.api.Assertions;
import org.yaml.snakeyaml.introspector.Property;

public class LoaderTest
{
    /**
     * This test mostly validates that DefaultLoader will match what SnakeYAML would do (if camel casing gets converted to snake),
     * if this test fails it is likely a new property was added and not properly picked up by one of the two implementations;
     * if you added a property and now this test is failing, make sure to follow all of the following rules:
     *
     * 1) public field
     * 2) getter AND setter
     * 3) if getter of Boolean, use "get" prefix rather than "is"
     */
    @Test
    public void allMatchProperties()
    {
        Class<?> klass = Config.class;
        // treeset makes error cleaner to read
        Set<String> properties = defaultImpl(klass).keySet();
        Assertions.assertThat(new TreeSet<>(properties))
                  .isEqualTo(new TreeSet<>(snake(klass).keySet()));
    }

    private static Map<String, Property> snake(Class<?> klass)
    {
        return new SnakeYamlLoader().flatten(klass);
    }

    private static Map<String, Property> defaultImpl(Class<?> klass)
    {
        return new DefaultLoader().flatten(klass);
    }
}