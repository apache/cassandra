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

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.TreeMap;

/**
 * properties that for system.compaction_history, and more properties can be add in the future
 * */
public class CompactionHistoryProperty
{
    // compaction properties
    public static final String COMPACTION_TYPE = "compaction_type";
    
    // define the default map for the compaction_properties column in system.compaction_history
    public static final Map<String, String> DEFAULT_COMPACTION_PROPERTIES = ImmutableMap.of(COMPACTION_TYPE, OperationType.UNKNOWN.type );
    
    /**
     * get the compaction history properties , if none value exists  a default value will return
     * @param propertiesColumnValueInput input properties map
     * @return the properties
     * */
    public static Map<String, String> getCompactionHistroyProperties(Map<String, String> propertiesColumnValueInput) 
    {
        if (propertiesColumnValueInput == null)
        {
            return DEFAULT_COMPACTION_PROPERTIES;
        }

        Map<String, String> result = new TreeMap<>();
        for(String key : DEFAULT_COMPACTION_PROPERTIES.keySet()) 
        {
            String value = propertiesColumnValueInput.get(key);
            if (value != null) 
            {
                result.put(key, value);
            }
        }
        return result;
    }
    
}
