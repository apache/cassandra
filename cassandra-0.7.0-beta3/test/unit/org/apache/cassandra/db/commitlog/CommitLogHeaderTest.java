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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;

public class CommitLogHeaderTest extends SchemaLoader
{
    
    @Test
    public void testEmptyHeader()
    {
        CommitLogHeader clh = new CommitLogHeader();
        assert clh.getReplayPosition() < 0;
    }
    
    @Test
    public void lowestPositionWithZero()
    {
        CommitLogHeader clh = new CommitLogHeader();
        clh.turnOn(2, 34);
        assert clh.getReplayPosition() == 34;
        clh.turnOn(100, 0);
        assert clh.getReplayPosition() == 0;
        clh.turnOn(65, 2);
        assert clh.getReplayPosition() == 0;
    }
}
