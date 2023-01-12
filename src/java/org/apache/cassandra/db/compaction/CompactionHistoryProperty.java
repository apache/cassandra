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

package org.apache.cassandra.db.compaction;

import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

/**
 * properties that for system.compaction_history, and more properties can be add in the future
 * */
public class CompactionHistoryProperty
{
    // define the map key for the compaction_properties column in system.compaction_history
    public static final String[] COMPACTION_PROPERTIES_KEYS = { "compaction_type" };
    // define the map value for the compaction_properties column in system.compaction_history
    public static final String[] DEFAULT_COMPACTION_PROPERTIES_VALUES = { OperationType.UNKNOWN.type };
    
    /**
     * get the compaction history properties , if none value exists  a default value will return
     * @param propertiesColumnValueInput input properties map
     * @return the properties
     * */
    public static Map<String, String> getCompactionHistroyProperties(Map<String, String> propertiesColumnValueInput)
    {
        assert COMPACTION_PROPERTIES_KEYS.length == DEFAULT_COMPACTION_PROPERTIES_VALUES.length;
        Map<String, String> propertiesColumnValueResult = new TreeMap<>();
        for(int i = 0; i != COMPACTION_PROPERTIES_KEYS.length; ++i)
        {
            if(propertiesColumnValueInput == null)
            {
                propertiesColumnValueResult.put(COMPACTION_PROPERTIES_KEYS[i], DEFAULT_COMPACTION_PROPERTIES_VALUES[i]);
                continue;
            }
            String value = propertiesColumnValueInput.get(COMPACTION_PROPERTIES_KEYS[i]);
            if(value == null)
            {
                value = DEFAULT_COMPACTION_PROPERTIES_VALUES[i];
            }
            propertiesColumnValueResult.put(COMPACTION_PROPERTIES_KEYS[i], value);
        }
        return propertiesColumnValueResult;
    }
    
}
