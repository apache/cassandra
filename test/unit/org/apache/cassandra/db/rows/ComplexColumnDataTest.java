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
    private ColumnMetadata column = ColumnMetadata.regularColumn("ks", "tab", "col",
                                                                 MapType.getInstance(Int32Type.instance, Int32Type.instance, true));

    @Test
    public void testEmptyComplexColumn()
    {
        ComplexColumnData data = new ComplexColumnData(column,
                                                       BTree.empty(),
                                                       DeletionTime.LIVE);
        Assert.assertFalse(data.hasCells());
    }

    @Test
    public void testNonEmptyComplexColumn()
    {

        ComplexColumnData data = new ComplexColumnData(column,
                                                       BTree.singleton("ignored value"),
                                                       DeletionTime.LIVE);
        Assert.assertTrue(data.hasCells());
    }
}