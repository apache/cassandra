/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.commitlog;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class CommitLogHeaderTest extends SchemaLoader
{
    
    @Test
    public void testEmptyHeader()
    {
        CommitLogHeader clh = new CommitLogHeader();
        assert CommitLogHeader.getLowestPosition(clh) == 0;
    }
    
    @Test
    public void lowestPositionWithZero()
    {
        // zero should never be the lowest position unless all positions are zero.
        CommitLogHeader clh = new CommitLogHeader();
        clh.turnOn(2, 34);
        assert CommitLogHeader.getLowestPosition(clh) == 34;
        clh.turnOn(100, 0);
        assert CommitLogHeader.getLowestPosition(clh) == 34;
        clh.turnOn(65, 2);
        assert CommitLogHeader.getLowestPosition(clh) == 2;
    }
    
    @Test
    public void lowestPositionEmpty()
    {
        CommitLogHeader clh = new CommitLogHeader();
        assert CommitLogHeader.getLowestPosition(clh) == 0;
    }
    
    @Test
    public void constantSize() throws IOException
    {
        CommitLogHeader clh = new CommitLogHeader();
        clh.turnOn(2, 34);
        byte[] one = clh.toByteArray();
        
        clh = new CommitLogHeader();
        for (int i = 0; i < 5; i++)
            clh.turnOn(i, 1000 * i);
        byte[] two = clh.toByteArray();
        
        assert one.length == two.length;
    }
    
    @Test
    public void cfMapSerialization() throws IOException
    {
        Map<Pair<String, String>, Integer> map = CFMetaData.getCfIdMap();
        CommitLogHeader clh = new CommitLogHeader();
        assert clh.getCfIdMap().equals(map);
    }
}
