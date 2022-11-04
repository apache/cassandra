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
package org.apache.cassandra.db.monitoring;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.cassandra.schema.TableMetadata;

public class MvInUse extends BadQueryTypes
{
    private static final Map<String, Integer> visitedMVs = new ConcurrentHashMap<>();

    public MvInUse(String keySpace,
                   String mvName)
    {
        super(keySpace, mvName, true);
    }

    @Override
    public void cleanup()
    {
        visitedMVs.clear();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append(", problemText:");
        sb.append("mv is experimental feature in Cassandra - mv should not be used; more details https://t.uber.com/cassandra-mv-experimental");
        return sb.toString();
    }

    public String getKey()
    {
        return "";
    }

    public String getDetails()
    {
        return "";
    }

    static void checkForMV(TableMetadata cfm)
    {
        if (cfm != null && cfm.isView())
        {
            if (!visitedMVs.containsKey(cfm.name))
            {
                visitedMVs.put(cfm.name, 0);
                BadQuery.report(BadQuery.BadQueryCategory.MV_IN_USE,
                                new MvInUse(cfm.keyspace, cfm.name));
            }
        }
    }
}

