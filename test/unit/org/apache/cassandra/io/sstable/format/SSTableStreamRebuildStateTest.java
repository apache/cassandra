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

package org.apache.cassandra.io.sstable.format;

import org.junit.Test;

import org.apache.cassandra.io.sstable.format.SSTableStreamRebuildState.State;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SSTableStreamRebuildStateTest
{
    @Test
    public void startsNormal()
    {
        SSTableStreamRebuildState s = new SSTableStreamRebuildState();
        assertEquals(State.NORMAL, s.state());
        assertEquals(0, s.zcsStreamCount());
    }

    @Test
    public void multipleStreamsAllowedAndCounted()
    {
        SSTableStreamRebuildState s = new SSTableStreamRebuildState();
        assertTrue(s.tryBeginStreaming());
        assertTrue(s.tryBeginStreaming());
        assertEquals(State.ZCS_STREAMING, s.state());
        assertEquals(2, s.zcsStreamCount());

        s.endStreaming();
        assertEquals(State.ZCS_STREAMING, s.state());
        assertEquals(1, s.zcsStreamCount());

        s.endStreaming();
        assertEquals(State.NORMAL, s.state());
        assertEquals(0, s.zcsStreamCount());
    }

    @Test
    public void rebuildBlockedWhileStreaming()
    {
        SSTableStreamRebuildState s = new SSTableStreamRebuildState();
        assertTrue(s.tryBeginStreaming());
        assertFalse(s.tryBeginRebuild());
        assertEquals(State.ZCS_STREAMING, s.state());
    }

    @Test
    public void streamingBlockedWhileRebuilding()
    {
        SSTableStreamRebuildState s = new SSTableStreamRebuildState();
        assertTrue(s.tryBeginRebuild());
        assertFalse(s.tryBeginStreaming());
        assertEquals(State.REBUILDING, s.state());
        assertEquals(0, s.zcsStreamCount());
    }

    @Test
    public void rebuildExcludesSecondRebuild()
    {
        SSTableStreamRebuildState s = new SSTableStreamRebuildState();
        assertTrue(s.tryBeginRebuild());
        assertFalse(s.tryBeginRebuild());
    }

    @Test
    public void rebuildResetsToNormal()
    {
        SSTableStreamRebuildState s = new SSTableStreamRebuildState();
        assertTrue(s.tryBeginRebuild());
        s.endRebuild();
        assertEquals(State.NORMAL, s.state());
        assertTrue(s.tryBeginStreaming());
    }

    @Test
    public void endIsDefensiveAgainstOverRelease()
    {
        SSTableStreamRebuildState s = new SSTableStreamRebuildState();
        s.endStreaming(); // no-op, must not go negative
        s.endRebuild();   // no-op
        assertEquals(State.NORMAL, s.state());
        assertEquals(0, s.zcsStreamCount());
    }
}
