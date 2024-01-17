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

package org.apache.cassandra.db.commitlog;

import java.io.IOException;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;

import static org.apache.cassandra.config.CassandraRelevantProperties.COMMITLOG_SKIP_FILE_ADVICE;
import static org.junit.Assert.assertEquals;

public class MemoryMappedSegmentStartupTest
{
    @After
    public void tearDown() throws Exception
    {
        COMMITLOG_SKIP_FILE_ADVICE.reset();
    }

    @Test
    public void shouldSetSkipFileAdviceTrueWithParameterTrue() throws IOException
    {
        COMMITLOG_SKIP_FILE_ADVICE.setBoolean(true);
        try (Cluster cluster = Cluster.build(1).start())
        {
            assertEquals(true, cluster.get(1).callOnInstance(() -> MemoryMappedSegment.skipFileAdviseToFreePageCache));
        }
    }

    @Test
    public void shouldSetSkipFileAdviceFalseWithParameterFalse() throws IOException
    {
        COMMITLOG_SKIP_FILE_ADVICE.setBoolean(false);
        try (Cluster cluster = Cluster.build(1).start())
        {
            assertEquals(false, cluster.get(1).callOnInstance(() -> MemoryMappedSegment.skipFileAdviseToFreePageCache));
        }
    }

    @Test
    public void shouldSetSkipFileAdviceFalseWithParameterMissing() throws IOException
    {
        try (Cluster cluster = Cluster.build(1).start())
        {
            assertEquals(false, cluster.get(1).callOnInstance(() -> MemoryMappedSegment.skipFileAdviseToFreePageCache));
        }
    }
}
