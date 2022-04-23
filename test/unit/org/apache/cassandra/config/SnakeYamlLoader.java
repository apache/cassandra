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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.introspector.BeanAccess;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;

import static org.apache.cassandra.utils.FBUtilities.camelToSnake;

public final class SnakeYamlLoader implements Loader
{
    private final Helper helper = new Helper();

    @Override
    public Map<String, Property> getProperties(Class<?> root)
    {
        return helper.getPropertiesMap(root, BeanAccess.DEFAULT);
    }

    private static class Helper extends PropertyUtils
    {
        @Override
        public Map<String, Property> getPropertiesMap(Class<?> type, BeanAccess bAccess)
        {
            Map<String, Property> map;
            try
            {
                map = super.getPropertiesMap(type, bAccess);
            }
            catch (YAMLException e)
            {
                // some classes take a string in constructor and output as toString, these should be treated like
                // primitive types
                if (e.getMessage() != null && e.getMessage().startsWith("No JavaBean properties found"))
                    return Collections.emptyMap();
                throw e;
            }
            // filter out ignores
            Set<String> ignore = new HashSet<>();
            Map<String, String> rename = new HashMap<>();
            map.values().forEach(p -> {
                if (shouldIgnore(p))
                {
                    ignore.add(p.getName());
                    return;
                }
                String snake = camelToSnake(p.getName());
                if (!p.getName().equals(snake))
                {
                    if (map.containsKey(snake))
                        ignore.add(p.getName());
                    else
                        rename.put(p.getName(), snake);
                }
            });
            ignore.forEach(map::remove);
            rename.forEach((previous, desired) -> map.put(desired, map.remove(previous)));
            return map;
        }

        private static boolean shouldIgnore(Property p)
        {
            return !p.isWritable() || p.getAnnotation(JsonIgnore.class) != null;
        }
    }
}
