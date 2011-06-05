package org.apache.cassandra.db;
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


import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CLibrary;

import static org.apache.cassandra.Util.column;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ScrubTest extends CleanupHelper
{
    public String TABLE = "Keyspace1";
    public String CF = "Standard1";
    public String CF2 = "Super5";
    public String CF3 = "Standard2";
    public String  corruptSSTableName;

    
    public void copySSTables() throws IOException 
    {
        String root = System.getProperty("corrupt-sstable-root");
        assert root != null;
        File rootDir = new File(root);
        assert rootDir.isDirectory();
        
        String[] destDirs = DatabaseDescriptor.getAllDataFileLocationsForTable(TABLE);
        assert destDirs != null;
        assert destDirs.length > 0;
       
        FileUtils.createDirectory(destDirs[0]);
        for (File srcFile : rootDir.listFiles())
        {
            if (srcFile.getName().equals(".svn"))
                continue;
            File destFile = new File(destDirs[0]+File.separator+srcFile.getName());
            CLibrary.createHardLinkWithExec(srcFile, destFile);
                        
            destFile = new File(destDirs[0]+File.separator+srcFile.getName());
                        
            assert destFile.exists() : destFile.getAbsoluteFile();
            
            if(destFile.getName().endsWith("Data.db"))
                corruptSSTableName = destFile.getCanonicalPath();
        }   

        assert corruptSSTableName != null;
    }
   
    @Test
    public void testScrubFile() throws Exception
    {        
        copySSTables();

        Table table = Table.open(TABLE);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(CF2);
        assert cfs.getSSTables().size() > 0;
      
        List<Row> rows;
        boolean caught = false;
        try
        {
             rows = cfs.getRangeSlice(ByteBufferUtil.bytes("1"), Util.range("", ""), 1000, new IdentityQueryFilter());
             fail("This slice should fail");
        }
        catch (NegativeArraySizeException e)
        {
            caught = true;
        }
        assert caught : "'corrupt' test file actually was not";
        
        CompactionManager.instance.performScrub(cfs);
        rows = cfs.getRangeSlice(ByteBufferUtil.bytes("1"), Util.range("", ""), 1000, new IdentityQueryFilter());
        assertEquals(100, rows.size());
    }
    
    
    @Test
    public void testScrubOneRow() throws IOException, ExecutionException, InterruptedException, ConfigurationException
    {
        CompactionManager.instance.disableAutoCompaction();
        Table table = Table.open(TABLE);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(CF);

        List<Row> rows;

        // insert data and verify we get it back w/ range query
        fillCF(cfs, 1);
        rows = cfs.getRangeSlice(null, Util.range("", ""), 1000, new IdentityQueryFilter());
        assertEquals(1, rows.size());

        CompactionManager.instance.performScrub(cfs);

        // check data is still there
        rows = cfs.getRangeSlice(null, Util.range("", ""), 1000, new IdentityQueryFilter());
        assertEquals(1, rows.size());
    }

    @Test
    public void testScrubDeletedRow() throws IOException, ExecutionException, InterruptedException, ConfigurationException
    {
        CompactionManager.instance.disableAutoCompaction();
        Table table = Table.open(TABLE);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(CF3);

        RowMutation rm;
        rm = new RowMutation(TABLE, ByteBufferUtil.bytes(1));
        ColumnFamily cf = ColumnFamily.create(TABLE, CF3);
        cf.delete(0, 1); // expired tombstone
        rm.add(cf);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();

        CompactionManager.instance.performScrub(cfs);
        assert cfs.getSSTables().isEmpty();
    }

    @Test
    public void testScrubMultiRow() throws IOException, ExecutionException, InterruptedException, ConfigurationException
    {
        CompactionManager.instance.disableAutoCompaction();
        Table table = Table.open(TABLE);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(CF);

        List<Row> rows;

        // insert data and verify we get it back w/ range query
        fillCF(cfs, 10);
        rows = cfs.getRangeSlice(null, Util.range("", ""), 1000, new IdentityQueryFilter());
        assertEquals(10, rows.size());

        CompactionManager.instance.performScrub(cfs);

        // check data is still there
        rows = cfs.getRangeSlice(null, Util.range("", ""), 1000, new IdentityQueryFilter());
        assertEquals(10, rows.size());
    }
      
    protected void fillCF(ColumnFamilyStore cfs, int rowsPerSSTable) throws ExecutionException, InterruptedException, IOException
    {
        for (int i = 0; i < rowsPerSSTable; i++)
        {
            String key = String.valueOf(i);
            // create a row and update the birthdate value, test that the index query fetches the new version
            RowMutation rm;
            rm = new RowMutation(TABLE, ByteBufferUtil.bytes(key));
            ColumnFamily cf = ColumnFamily.create(TABLE, CF);
            cf.addColumn(column("c1", "1", 1L));
            cf.addColumn(column("c2", "2", 1L));
            rm.add(cf);
            rm.applyUnsafe();
        }

        cfs.forceBlockingFlush();
    }

    
    
    
}
