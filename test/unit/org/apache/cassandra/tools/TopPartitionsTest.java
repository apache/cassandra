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

package org.apache.cassandra.tools;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.management.openmbean.CompositeData;

import com.google.common.collect.Lists;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.StorageService;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.junit.Assert.assertEquals;

public class TopPartitionsTest
{
    @BeforeClass
    public static void loadSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
    }

    @Test
    public void testServiceTopPartitionsNoArg() throws Exception
    {
        BlockingQueue<Map<String, List<CompositeData>>> q = new ArrayBlockingQueue<>(1);
        ColumnFamilyStore.all();
        Executors.newCachedThreadPool().execute(() ->
        {
            try
            {
                q.put(StorageService.instance.samplePartitions(1000, 100, 10, Lists.newArrayList("READS", "WRITES")));
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        });
        Thread.sleep(100);
        SystemKeyspace.persistLocalMetadata();
        Map<String, List<CompositeData>> result = q.poll(5, TimeUnit.SECONDS);
        List<CompositeData> cd = result.get("WRITES");
        assertEquals(1, cd.size());
    }

    @Test
    public void testServiceTopPartitionsSingleTable() throws Exception
    {
        ColumnFamilyStore.getIfExists("system", "local").beginLocalSampling("READS", 5, 240000);
        String req = "SELECT * FROM system.%s WHERE key='%s'";
        executeInternal(format(req, SystemKeyspace.LOCAL, SystemKeyspace.LOCAL));
        List<CompositeData> result = ColumnFamilyStore.getIfExists("system", "local").finishLocalSampling("READS", 5);
        assertEquals("If this failed you probably have to raise the beginLocalSampling duration", 1, result.size());
    }
}
