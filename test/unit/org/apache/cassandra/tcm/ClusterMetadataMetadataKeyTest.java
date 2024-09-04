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

package org.apache.cassandra.tcm;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.Assertions;

/**
 * This test is to make sure that the fields of {@link ClusterMetadata} have a matching {@link MetadataKey} and the
 * utility functions linking key to field are maintained.
 *
 * If this test is failing it likely means a new field was added to {@link ClusterMetadata} and {@link MetadataKeys} was
 * not updated to know about it.
 */
public class ClusterMetadataMetadataKeyTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
    }

    private static final Map<String, Field> NAME_TO_KEY;

    static
    {
        ImmutableMap.Builder<String, Field> builder = ImmutableMap.builder();
        for (Field field : MetadataKeys.class.getDeclaredFields())
        {
            if (field.getType() == MetadataKey.class
                && Modifier.isStatic(field.getModifiers())
                && Modifier.isPublic(field.getModifiers()))
                builder.put(field.getName(), field);
        }
        NAME_TO_KEY = builder.build();
    }

    @Test
    public void metadataKeyExists() throws IllegalAccessException
    {
        ClusterMetadata empty = new ClusterMetadata(Murmur3Partitioner.instance);
        // Theese are fields that should not have MetadataKeys and should be ignored.
        Set<String> exclude = ImmutableSet.of("metadataIdentifier",
                                              "epoch",
                                              "partitioner",
                                              "extensions");
        // Mapping of ClusterMetadata field names to MetadataKey name; mapping is only needed if the names don't match.
        Map<String, String> mapping = ImmutableMap.of("directory", "node_directory",
                                                      "placements", "data_placements");
        for (Field field : ClusterMetadata.class.getDeclaredFields())
        {
            if (Modifier.isStatic(field.getModifiers())
                || !Modifier.isPublic(field.getModifiers())
                || !Modifier.isFinal(field.getModifiers()))
                continue;
            String name = field.getName();
            if (exclude.contains(name)) continue;
            if (mapping.containsKey(name))
                name = mapping.get(name);
            String snakeName = FBUtilities.camelToSnake(name).toUpperCase(Locale.ROOT);
            Assertions.assertThat(NAME_TO_KEY.keySet())
                      .describedAs("Unable to locate MetadataKey for %s", snakeName)
                      .contains(snakeName);
            MetadataKey expectedKey = (MetadataKey) NAME_TO_KEY.get(snakeName).get(null);
            if (!MetadataKeys.CORE_METADATA.containsKey(expectedKey))
                throw new IllegalStateException("MetadataKeys.CORE_METADATA is missing key " + expectedKey + " for field " + name);

            Assertions.assertThat(field.get(empty))
                      .describedAs("Extraction function does not seem to match the field %s and key %s", name, snakeName)
                      .isSameAs(MetadataKeys.CORE_METADATA.get(expectedKey).apply(empty));
        }
    }
}
