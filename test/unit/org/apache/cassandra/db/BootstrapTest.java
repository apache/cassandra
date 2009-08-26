/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.StringToken;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.io.StreamContextManager;
import org.junit.Test;

public class BootstrapTest
{
    /**
     * Writes out a bunch of keys into an SSTable, then runs anticompaction on a range.
     * Checks to see if anticompaction returns true.
     */
    private void testAntiCompaction(String columnFamilyName, int insertsPerTable) throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore(columnFamilyName);

       
        for (int j = 0; j < insertsPerTable; j++) 
        {
            String key = String.valueOf(j);
            RowMutation rm = new RowMutation("Keyspace1", key);
            rm.add(new QueryPath(columnFamilyName, null, "0".getBytes()), new byte[0], j);
            rm.apply();
        }
        
        store.forceBlockingFlush();
        List<String> fileList = new ArrayList<String>();
        List<Range> ranges  = new ArrayList<Range>();
        Range r = new Range(new StringToken("0"), new StringToken("zzzzzz"));
        ranges.add(r);

        boolean result = store.doAntiCompaction(ranges, new EndPoint("127.0.0.1", 9150), fileList);

        assertEquals(true, result); // some keys should have qualified
        assertEquals(true, fileList.size() >= 3); //Data, index, filter files
    }

    @Test
    public void testAntiCompaction1() throws IOException, ExecutionException, InterruptedException
    {
        testAntiCompaction("Standard1", 100);
    }
    
    @Test
    public void testGetNewNames() throws IOException
    {
        StreamContextManager.StreamContext[] streamContexts = new StreamContextManager.StreamContext[3];
        streamContexts[0] = new StreamContextManager.StreamContext("/foo/Standard1-500-Data.db", 100, "Keyspace1");
        streamContexts[1] = new StreamContextManager.StreamContext("/foo/Standard1-500-Index.db", 100, "Keyspace1");
        streamContexts[2] = new StreamContextManager.StreamContext("/foo/Standard1-500-Filter.db", 100, "Keyspace1");
        Table.BootStrapInitiateVerbHandler bivh = new Table.BootStrapInitiateVerbHandler();
        Map<String, String> fileNames = bivh.getNewNames(streamContexts);
        String result = fileNames.get("Keyspace1-Standard1-500");
        assertEquals(true, result.contains("Standard1"));
        assertEquals(true, result.contains("Data.db"));
        assertEquals(1, fileNames.entrySet().size());
        
        assertTrue( new File(bivh.getNewFileNameFromOldContextAndNames(fileNames, streamContexts[0])).getName().matches("Standard1-tmp-\\d+-Data.db"));
        assertTrue( new File(bivh.getNewFileNameFromOldContextAndNames(fileNames, streamContexts[1])).getName().matches("Standard1-tmp-\\d+-Index.db"));
        assertTrue( new File(bivh.getNewFileNameFromOldContextAndNames(fileNames, streamContexts[2])).getName().matches("Standard1-tmp-\\d+-Filter.db"));
    }

    
}
