package org.apache.cassandra.service;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

import java.util.List;
import java.util.LinkedList;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.thrift.ConsistencyLevel;

public class AntiEntropyServiceCounterTest extends AntiEntropyServiceTestAbstract
{
    public void init()
    {
        tablename = "Keyspace5";
        cfname    = "Counter1";
    }

    public List<IMutation> getWriteData()
    {
        List<IMutation> rms = new LinkedList<IMutation>();
        RowMutation rm = new RowMutation(tablename, ByteBufferUtil.bytes("key1"));
        rm.addCounter(new QueryPath(cfname, null, ByteBufferUtil.bytes("Column1")), 42);
        rms.add(new CounterMutation(rm, ConsistencyLevel.ONE));
        return rms;
    }
}
