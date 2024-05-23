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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.FBUtilities;

public class CompactionStrategyMigrationOptions
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionStrategyMigrationOptions.class);

    public final boolean enabled;

    public final String compaction_params_json;

    public Map<String, String> keyspace_options = new HashMap<>();

    public Map<String, String> table_options = new HashMap<>();

    public CompactionStrategyMigrationOptions()
    {
        this(false);
    }

    public CompactionStrategyMigrationOptions(boolean enabled)
    {
        this(enabled, "{\"class\": \"org.apache.cassandra.db.compaction.LeveledCompactionStrategy\"}");
    }

    public CompactionStrategyMigrationOptions(boolean enabled, String compactionParamsJson)
    {
        this.enabled = enabled;
        this.compaction_params_json = compactionParamsJson;
    }

    public static boolean isJsonValid(String json)
    {
        try
        {
            parseCompactionParamsJson(json).validate();
        }
        catch (Exception e)
        {
            logger.warn("exception occured when validating compaction params Json: ", e);
            return false;
        }
        return true;
    }

    public static CompactionParams parseCompactionParamsJson(String paramsJson)
    {
        return CompactionParams.fromMap(FBUtilities.fromJsonMap(paramsJson));
    }
}
