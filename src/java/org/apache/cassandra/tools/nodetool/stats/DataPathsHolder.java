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
package org.apache.cassandra.tools.nodetool.stats;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.tools.NodeProbe;

public class DataPathsHolder implements StatsHolder
{
    public final Map<String, Object> pathsHash;

    public DataPathsHolder(NodeProbe probe, List<String> tableNames)
    {
        this.pathsHash = new HashMap();

        Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> tableMBeans = probe.getColumnFamilyStoreMBeanProxies();
        while (tableMBeans.hasNext())
        {
            Map.Entry<String, ColumnFamilyStoreMBean> entry = tableMBeans.next();
            String keyspaceName = entry.getKey();
            String tableName = entry.getValue().getTableName();

            if (!(tableNames.isEmpty() || 
                  tableNames.contains(keyspaceName + "." + tableName) || 
                  tableNames.contains(keyspaceName) ))
            {
                continue;
            }

            Map<String, List<String>> ksPaths;
            List<String> dataPaths;

            try
            {
                dataPaths = entry.getValue().getDataPaths();
            }
            catch (IOException ex)
            {
                continue;
            }

            if (pathsHash.containsKey(keyspaceName))
            {
                ksPaths = (Map<String, List<String>>) pathsHash.get(keyspaceName);
            }
            else
            {
                ksPaths = new HashMap();
                pathsHash.put(keyspaceName, ksPaths);
            }
            ksPaths.put(tableName, dataPaths);
        }
    }

    @Override
    public Map<String, Object> convert2Map()
    {
        return this.pathsHash;
    }
}
