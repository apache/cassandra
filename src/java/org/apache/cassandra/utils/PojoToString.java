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

package org.apache.cassandra.utils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.DumperOptions.FlowStyle;
import org.yaml.snakeyaml.Yaml;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.cassandra.utils.JsonUtils.JSON_OBJECT_MAPPER;

/**
 * Helper to format POJOs that are easy to convert (primitives, nonnull, and built in collections)
 * in various human + machine readable formats. Useful for JMX and nodetool.
 */
public class PojoToString
{
    public static final Integer VERSION_50 = 0;

    public static final Integer CURRENT_VERSION = VERSION_50;

    enum Format
    {
        YAML,
        MINIFIED_YAML(true),
        JSON,
        MINIFIED_JSON(true);

        boolean minified;

        Format()
        {
            this(false);
        }

        Format(boolean minified)
        {
            this.minified = minified;
        }

        public boolean isYaml()
        {
            return this == YAML || this == MINIFIED_YAML;
        }

        public static Format fromString(String formatString)
        {
            formatString = formatString.toUpperCase();
            switch (formatString)
            {
                case "YAML":
                    return YAML;
                case "MINIFIED-YAML":
                    return MINIFIED_YAML;
                case "JSON":
                    return JSON;
                case "MINIFIED-JSON":
                    return MINIFIED_JSON;
                default: throw new IllegalArgumentException("Unsupported format " + formatString
                                                            + " supported formats are YAML, MINIFIED-YAML, JSON, MINIFIED-JSON");
            }
        }
    }
    private static final Set<Class<?>> ALLOWED_PRIMITIVES = ImmutableSet.of(
        String.class,
        Double.class,
        Float.class,
        Long.class,
        Integer.class,
        Short.class,
        Byte.class
    );

    private static final List<Class<?>> ALLOWED_COLLECTIONS = ImmutableList.of(
        List.class,
        Set.class
    );

    /**
     * Helper to convert POJOs from a restricted set (primitive Java types and collections) to a human/machine readable
     * format that is specified by the format parameter.
     *
     * This doesn't enforce what objects are serialized so you can get error or messy output if you try and serialize
     * things that aren't primitive or collections.
     *
     * The map must contain a 'version' key set to CURRENT_VERSION
     * @param map Map POJO that must be restricted to easily representable types (map, set , list, primitives), and contains the 'version' key set to CURRENT_VERSION
     * @param formatString Human/machine readable format name, can be YAML or JSON, prefix with MINIFIED- to get a minified version
     * @return The map formatted in the requested format
     * @throws IllegalArgumentException If the 'version' key is not present and set to CURRENT_VERSION
     */
    public static String pojoMapToString(Map<String, Object> map, String formatString)
    {
        checkArgument(CURRENT_VERSION.equals(map.get("version")));
        return pojoToString(map, formatString);
    }

    private static String pojoToString(Object obj, String formatString)
    {
        validateAllowedTypes(obj);
        Format format = Format.fromString(formatString);
        if (format.isYaml())
        {
            DumperOptions dumperOptions = new DumperOptions();
            if (format.minified)
            {
                dumperOptions.setDefaultFlowStyle(FlowStyle.FLOW);
                dumperOptions.setIndent(1);
                dumperOptions.setWidth(Integer.MAX_VALUE);
                dumperOptions.setSplitLines(false);
            }
            // TODO How do you get snake yaml to produce minified output?
            return new Yaml(dumperOptions).dump(obj);
        }
        else
        {
            try
            {
                if (format.minified)
                    return JSON_OBJECT_MAPPER.writeValueAsString(obj);
                else
                    return JSON_OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
            }
            catch (JsonProcessingException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private static void validateAllowedTypes(Object o)
    {
        if (o == null)
            throw new NullPointerException("Null objects are unsupported");
        if (o instanceof Map)
        {
            for (Map.Entry<Object, Object> entry : ((Map<Object, Object>)o).entrySet())
            {
                Object key = entry.getKey();
                if (!(key instanceof String | key instanceof Long | key instanceof Integer))
                    throw new IllegalArgumentException("Map has entry with key " + entry.getKey() + " of "
                                                       + key.getClass() + " which is unsupported, only String is supported for map keys");
                validateAllowedTypes(entry.getValue());
            }
        }
        else if (o instanceof Collection)
        {
            if (!(o instanceof Set | o instanceof List))
                throw new IllegalArgumentException("Collection " + o + " with " + o.getClass() + " is not in allow list " + ALLOWED_COLLECTIONS);
            for (Object element : ((Collection)o))
                validateAllowedTypes(element);
        }
        else if (!ALLOWED_PRIMITIVES.contains(o.getClass()))
            throw new IllegalArgumentException("Scalar " + o + " with " + o.getClass() + " is not in allow list " + ALLOWED_PRIMITIVES);

    }
}
