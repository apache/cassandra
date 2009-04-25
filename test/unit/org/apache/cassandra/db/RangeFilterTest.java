/**
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

package org.apache.cassandra.db;

import java.io.IOException;


import org.testng.Assert;
import org.testng.annotations.Test;


public class RangeFilterTest
{
    @Test
    public void testRangeFilterOnColumns() throws IOException
    {
        ColumnFamily cf = new ColumnFamily("Standard1", "Standard");
        byte[] val = "test value".getBytes();
        cf.addColumn(new Column("a", val, System.currentTimeMillis()));
        cf.addColumn(new Column("b", val, System.currentTimeMillis()));
        cf.addColumn(new Column("c", val, System.currentTimeMillis()));
        cf.addColumn(new Column("d", val, System.currentTimeMillis()));
        cf.addColumn(new Column("e", val, System.currentTimeMillis()));

        IFilter f = new RangeFilter("b", "d");
        ColumnFamily filteredCf = f.filter(cf.name(), cf);
        
        Assert.assertEquals(filteredCf.getColumnCount(),3);
        Assert.assertFalse(f.isDone());
    }
    
    @Test
    public void testRangeFilterOnColumnsWithCount() throws IOException
    {
        ColumnFamily cf = new ColumnFamily("Standard1", "Standard");
        byte[] val = "test value".getBytes();
        cf.addColumn(new Column("a", val, System.currentTimeMillis()));
        cf.addColumn(new Column("b", val, System.currentTimeMillis()));
        cf.addColumn(new Column("c", val, System.currentTimeMillis()));
        cf.addColumn(new Column("d", val, System.currentTimeMillis()));
        cf.addColumn(new Column("e", val, System.currentTimeMillis()));

        IFilter f = new RangeFilter("b", "d", 2);
        ColumnFamily filteredCf = f.filter(cf.name(), cf);
        
        Assert.assertEquals(filteredCf.getColumnCount(),2);
        Assert.assertTrue(f.isDone());
    }

    @Test
    public void testRangeFilterOnSuperColumns() throws IOException
    {
        ColumnFamily cf = new ColumnFamily("Super1", "Super");
        byte[] val = "test value".getBytes();
        SuperColumn sc = null;
        sc = new SuperColumn("a");
        sc.addColumn("a1", new Column("a1", val, System.currentTimeMillis()));
        sc.addColumn("a2", new Column("a2", val, System.currentTimeMillis()));
        cf.addColumn(sc);
        sc = new SuperColumn("b");
        sc.addColumn("b1", new Column("b1", val, System.currentTimeMillis()));
        sc.addColumn("b2", new Column("b2", val, System.currentTimeMillis()));
        cf.addColumn(sc);
        sc = new SuperColumn("c");
        sc.addColumn("c1", new Column("c1", val, System.currentTimeMillis()));
        sc.addColumn("c2", new Column("c2", val, System.currentTimeMillis()));
        cf.addColumn(sc);
        sc = new SuperColumn("d");
        sc.addColumn("d1", new Column("d1", val, System.currentTimeMillis()));
        sc.addColumn("d2", new Column("d2", val, System.currentTimeMillis()));
        cf.addColumn(sc);
        sc = new SuperColumn("e");
        sc.addColumn("e1", new Column("e1", val, System.currentTimeMillis()));
        sc.addColumn("e2", new Column("e2", val, System.currentTimeMillis()));
        cf.addColumn(sc);

        IFilter f = new RangeFilter("b", "d");
        ColumnFamily filteredCf = f.filter(cf.name(), cf);

        IColumn col = filteredCf.getColumn("a");
        Assert.assertNull(col);

        col = filteredCf.getColumn("e");
        Assert.assertNull(col);

        col = filteredCf.getColumn("c");
        Assert.assertNotNull(col);
        Assert.assertFalse(f.isDone());
    }

}
