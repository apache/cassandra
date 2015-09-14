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
package org.apache.cassandra.io.sstable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.io.sstable.IndexHelper.IndexInfo;
import static org.junit.Assert.assertEquals;

public class IndexHelperTest
{

    private static ClusteringComparator comp = new ClusteringComparator(Collections.<AbstractType<?>>singletonList(LongType.instance));
    private static ClusteringPrefix cn(long l)
    {
        return Util.clustering(comp, l);
    }

    @Test
    public void testIndexHelper()
    {
        DeletionTime deletionInfo = new DeletionTime(FBUtilities.timestampMicros(), FBUtilities.nowInSeconds());

        List<IndexInfo> indexes = new ArrayList<>();
        indexes.add(new IndexInfo(cn(0L), cn(5L), 0, 0, deletionInfo));
        indexes.add(new IndexInfo(cn(10L), cn(15L), 0, 0, deletionInfo));
        indexes.add(new IndexInfo(cn(20L), cn(25L), 0, 0, deletionInfo));

        assertEquals(0, IndexHelper.indexFor(cn(-1L), indexes, comp, false, -1));
        assertEquals(0, IndexHelper.indexFor(cn(5L), indexes, comp, false, -1));
        assertEquals(1, IndexHelper.indexFor(cn(12L), indexes, comp, false, -1));
        assertEquals(2, IndexHelper.indexFor(cn(17L), indexes, comp, false, -1));
        assertEquals(3, IndexHelper.indexFor(cn(100L), indexes, comp, false, -1));
        assertEquals(3, IndexHelper.indexFor(cn(100L), indexes, comp, false, 0));
        assertEquals(3, IndexHelper.indexFor(cn(100L), indexes, comp, false, 1));
        assertEquals(3, IndexHelper.indexFor(cn(100L), indexes, comp, false, 2));
        assertEquals(3, IndexHelper.indexFor(cn(100L), indexes, comp, false, 3));

        assertEquals(-1, IndexHelper.indexFor(cn(-1L), indexes, comp, true, -1));
        assertEquals(0, IndexHelper.indexFor(cn(5L), indexes, comp, true, 3));
        assertEquals(0, IndexHelper.indexFor(cn(5L), indexes, comp, true, 2));
        assertEquals(1, IndexHelper.indexFor(cn(17L), indexes, comp, true, 3));
        assertEquals(2, IndexHelper.indexFor(cn(100L), indexes, comp, true, 3));
        assertEquals(2, IndexHelper.indexFor(cn(100L), indexes, comp, true, 4));
        assertEquals(1, IndexHelper.indexFor(cn(12L), indexes, comp, true, 3));
        assertEquals(1, IndexHelper.indexFor(cn(12L), indexes, comp, true, 2));
        assertEquals(1, IndexHelper.indexFor(cn(100L), indexes, comp, true, 1));
        assertEquals(2, IndexHelper.indexFor(cn(100L), indexes, comp, true, 2));
    }
}
