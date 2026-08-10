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

package org.apache.cassandra.index.sai;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.vector.JVectorVersionUtil;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.ReflectionUtils;

public class SAIUtil
{
    public static Version currentVersion()
    {
        return Version.current(CQLTester.KEYSPACE);
    }

    public static void resetCurrentVersion()
    {
        setCurrentVersion(Version.Selector.DEFAULT);
    }

    public static void setCurrentVersion(Version version)
    {
        setCurrentVersion(version, Map.of());
    }

    public static void setCurrentVersion(Version defaultVersion, Map<String, Version> versionsPerKeyspace)
    {
        setCurrentVersion(new CustomVersionSelector(defaultVersion, versionsPerKeyspace));
    }

    public static void setCurrentVersion(Version.Selector versionSelector)
    {
        try
        {
            // set the current version
            Field field = Version.class.getDeclaredField("SELECTOR");
            field.setAccessible(true);
            Field modifiersField = ReflectionUtils.getField(Field.class, "modifiers");
            modifiersField.setAccessible(true);
            field.set(null, versionSelector);

            // update the index contexts for each keyspace
            for (String keyspaceName : Schema.instance.getKeyspaces())
            {
                Keyspace keyspace = Keyspace.open(keyspaceName);
                Version version = versionSelector.select(keyspaceName);

                for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                {
                    SecondaryIndexManager sim = cfs.getIndexManager();
                    for (Index index : sim.listIndexes())
                    {
                        if (index instanceof StorageAttachedIndex)
                        {
                            StorageAttachedIndex sai = (StorageAttachedIndex)index;
                            IndexContext context = sai.getIndexContext();

                            field = IndexContext.class.getDeclaredField("version");
                            field.setAccessible(true);
                            field.set(context, version);

                            field = IndexContext.class.getDeclaredField("primaryKeyFactory");
                            field.setAccessible(true);
                            field.set(context, version.onDiskFormat().newPrimaryKeyFactory(cfs.metadata().comparator));
                        }
                    }
                }
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void setEnableFused(boolean enableFused)
    {
        try
        {
            CassandraRelevantProperties.SAI_VECTOR_ENABLE_FUSED.setBoolean(enableFused);
            Field field = JVectorVersionUtil.class.getDeclaredField("ENABLE_FUSED");
            field.setAccessible(true);
            Field modifiersField = ReflectionUtils.getField(Field.class, "modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            field.set(null, enableFused);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void setEnableNVQ(boolean enableNVQ)
    {
        try
        {
            CassandraRelevantProperties.SAI_VECTOR_ENABLE_NVQ.setBoolean(enableNVQ);
            Field field = JVectorVersionUtil.class.getDeclaredField("ENABLE_NVQ");
            field.setAccessible(true);
            Field modifiersField = ReflectionUtils.getField(Field.class, "modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            field.set(null, enableNVQ);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static class CustomVersionSelector implements Version.Selector
    {
        private final Version defaultVersion;
        private final Map<String, Version> versionsPerKeyspace;

        public CustomVersionSelector(Version defaultVersion, Map<String, Version> versionsPerKeyspace)
        {
            this.defaultVersion = defaultVersion;
            this.versionsPerKeyspace = versionsPerKeyspace;
        }

        @Override
        public Version select(String keyspace)
        {
            return versionsPerKeyspace.getOrDefault(keyspace, defaultVersion);
        }
    }
}
