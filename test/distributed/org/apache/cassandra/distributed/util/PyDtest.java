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

package org.apache.cassandra.distributed.util;

import java.util.Arrays;
import java.util.List;

public class PyDtest
{

    public static class CreateCf
    {
        final String keyspace;
        final String name;
        String primaryKey, clustering, keyType, speculativeRetry, compression, validation, compactionStrategy;
        Float readRepair;
        Integer gcGrace;
        List<String> columns;
        Boolean compactStorage;

        public CreateCf(String keyspace, String name)
        {
            this.keyspace = keyspace;
            this.name = name;
        }

        public CreateCf withPrimaryKey(String primaryKey)
        {
            this.primaryKey = primaryKey;
            return this;
        }

        public CreateCf withClustering(String clustering)
        {
            this.clustering = clustering;
            return this;
        }

        public CreateCf withKeyType(String keyType)
        {
            this.keyType = keyType;
            return this;
        }

        public CreateCf withSpeculativeRetry(String speculativeRetry)
        {
            this.speculativeRetry = speculativeRetry;
            return this;
        }

        public CreateCf withCompression(String compression)
        {
            this.compression = compression;
            return this;
        }

        public CreateCf withValidation(String validation)
        {
            this.validation = validation;
            return this;
        }

        public CreateCf withCompactionStrategy(String compactionStrategy)
        {
            this.compactionStrategy = compactionStrategy;
            return this;
        }

        public CreateCf withReadRepair(Float readRepair)
        {
            this.readRepair = readRepair;
            return this;
        }

        public CreateCf withGcGrace(Integer gcGrace)
        {
            this.gcGrace = gcGrace;
            return this;
        }

        public CreateCf withColumns(List<String> columns)
        {
            this.columns = columns;
            return this;
        }

        public CreateCf withColumns(String ... columns)
        {
            this.columns = Arrays.asList(columns);
            return this;
        }

        public CreateCf withCompactStorage(Boolean compactStorage)
        {
            this.compactStorage = compactStorage;
            return this;
        }

        public String build()
        {
            if (keyspace == null)
                throw new IllegalArgumentException();
            if (name == null)
                throw new IllegalArgumentException();
            if (keyType == null)
                keyType = "varchar";
            if (validation == null)
                validation = "UTF8Type";
            if (compactionStrategy == null)
                compactionStrategy = "SizeTieredCompactionStrategy";
            if (compactStorage == null)
                compactStorage = false;


            String compaction_fragment = String.format("compaction = {'class': '%s', 'enabled': 'true'}", compactionStrategy);

            String query;
            String additional_columns = "";
            if (columns == null)
            {
                query = String.format("CREATE COLUMNFAMILY %s.%s (key %s, c varchar, v varchar, PRIMARY KEY(key, c)) WITH comment=\'test cf\'", keyspace, name, keyType);
            }
            else
            {
                for (String pair : columns)
                {
                    String[] split = pair.split(":");
                    String key = split[0];
                    String type = split[1];
                    additional_columns += ", " + key + " " + type;
                }

                if (primaryKey != null)
                    query = String.format("CREATE COLUMNFAMILY %s.%s (key %s%s, PRIMARY KEY(%s)) WITH comment=\'test cf\'", keyspace, name, keyType, additional_columns, primaryKey);
                else
                    query = String.format("CREATE COLUMNFAMILY %s.%s (key %s PRIMARY KEY%s) WITH comment=\'test cf\'", keyspace, name, keyType, additional_columns);
            }


            if (compaction_fragment != null)
                query += " AND " + compaction_fragment;

            if (clustering != null)
                query += String.format(" AND CLUSTERING ORDER BY (%s)", clustering);

            if (compression != null)
                query += String.format(" AND compression = { \'class\': \'%sCompressor\' }", compression);
            else
                query += " AND compression = {}";

            if (readRepair != null)
                query += String.format(" AND read_repair_chance=%f AND dclocal_read_repair_chance=%f", readRepair, readRepair);
            if (gcGrace != null)
                query += String.format(" AND gc_grace_seconds=%d", gcGrace);
            if (speculativeRetry != null)
                query += String.format(" AND speculative_retry=\'%s\'", speculativeRetry);

            if (compactStorage != null && compactStorage)
                query += " AND COMPACT STORAGE";

            return query;
        }
    }

    public static CreateCf createCf(String keyspace, String name)
    {
        return new CreateCf(keyspace, name);
    }

}
