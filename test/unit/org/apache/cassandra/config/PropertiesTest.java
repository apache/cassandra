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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.Test;

import org.yaml.snakeyaml.introspector.Property;

import static org.assertj.core.api.Assertions.assertThat;

public class PropertiesTest
{
    private final Loader loader = Properties.defaultLoader();

    @Test
    public void backAndForth() throws Exception
    {
        Map<String, Property> ps = loader.flatten(Config.class);

        Config config = new Config();
        Set<String> keys = ImmutableSet.of("server_encryption_options.enabled",
                                           "client_encryption_options.enabled",
                                           "server_encryption_options.optional",
                                           "client_encryption_options.optional");
        for (Property prop : ps.values())
        {
            // skip these properties as they don't allow get/set within the context of this test
            if (keys.contains(prop.getName()))
                continue;
            Object value = prop.get(config);
            if (value == null)
                continue;
            prop.set(config, value);
            Object back = prop.get(config);
            assertThat(back).isEqualTo(value);
        }
    }

    @Test
    public void configMutate() throws Exception
    {
        Map<String, Property> ps = loader.getProperties(Config.class);
        assertThat(ps)
                  .isNotEmpty()
                  .containsKey("cluster_name") // string / primitive
                  .containsKey("permissions_validity") // SmallestDurationSeconds
                  .containsKey("hinted_handoff_disabled_datacenters") // set<string>
                  .containsKey("disk_access_mode"); // enum

        // can we mutate the config?
        Config config = new Config();

        ps.get("cluster_name").set(config, "properties testing");
        assertThat(config.cluster_name).isEqualTo("properties testing");

        ps.get("permissions_validity").set(config, new DurationSpec.IntMillisecondsBound(42));
        assertThat(config.permissions_validity.toMilliseconds()).isEqualTo(42);

        ps.get("hinted_handoff_disabled_datacenters").set(config, Sets.newHashSet("a", "b", "c"));
        assertThat(config.hinted_handoff_disabled_datacenters).isEqualTo(Sets.newHashSet("a", "b", "c"));

        ps.get("disk_access_mode").set(config, Config.DiskAccessMode.mmap);
        assertThat(config.disk_access_mode).isEqualTo(Config.DiskAccessMode.mmap);
    }

    @Test
    public void nestedMutate() throws Exception
    {
        Map<String, Property> ps = loader.flatten(Config.class);

        assertThat(ps)
        .containsKey("seed_provider.class_name");

        // can we mutate the config?
        Config config = new Config();

        ps.get("seed_provider.class_name").set(config, "testing");
        assertThat(config.seed_provider.class_name).isEqualTo("testing");
    }
}