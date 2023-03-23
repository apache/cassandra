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

package org.apache.cassandra.service.metadata;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

public abstract class AbstractMetadataProvider implements MetadataProvider
{
    public Map<String, String> parse(String rawMetadata)
    {
        if (rawMetadata == null)
            return ImmutableMap.of();

        String[] pairs = rawMetadata.trim().split(",");
        Map<String, String> map = new HashMap<>();

        for (String pair : pairs)
        {
            String[] p = pair.split("=");
            if (p.length != 2)
                continue;

            String key = p[0].trim();
            String value = p[1].trim();

            if (key.length() == 0)
                continue;

            map.put(key, value);
        }

        return map;
    }
}
