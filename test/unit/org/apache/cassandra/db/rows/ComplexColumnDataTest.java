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

package org.apache.cassandra.db.rows;

import org.junit.Assert;
import org.junit.Test;

import junit.framework.TestCase;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.btree.BTree;

public class ComplexColumnDataTest extends TestCase
{
    private ColumnMetadata complexColumn = ColumnMetadata.regularColumn("ks", "tab", "col0",
                                                                        MapType.getInstance(Int32Type.instance, Int32Type.instance, true));

    private ColumnMetadata simpleColumn = ColumnMetadata.regularColumn("ks", "tab", "col1",
                                                                       Int32Type.instance);

    @Test
    public void testEmptyComplexColumn()
    {
        ComplexColumnData data = new ComplexColumnData(complexColumn,
                                                       BTree.empty(),
                                                       DeletionTime.LIVE);
        Assert.assertFalse(data.hasCells());
    }

    @Test
    public void testNonEmptyComplexColumn()
    {

        ComplexColumnData data = new ComplexColumnData(complexColumn,
                                                       BTree.singleton("ignored value"),
                                                       DeletionTime.LIVE);
        Assert.assertTrue(data.hasCells());
    }

    @Test
    public void testComplexColumnMinTimestampWithDeletion()
    {
        ComplexColumnData data = new ComplexColumnData(complexColumn,
                                                       BTree.empty(),
                                                       new DeletionTime(500, 1000));
        Assert.assertEquals("Min timestamp must be equal to deletion timestamp", 500, data.minTimestamp());
    }

    @Test
    public void testComplexColumnMinTimestampWithCells()
    {
        ComplexColumnData data = new ComplexColumnData(complexColumn,
                                                       new Cell[]{ new BufferCell(simpleColumn, 100, 0, 200, null, null) },
                                                       new DeletionTime(500, 1000));
        Assert.assertEquals("Min timestamp must be equal to min cell timestamp", 100, data.minTimestamp());
    }
}