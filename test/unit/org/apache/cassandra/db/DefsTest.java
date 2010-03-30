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

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.config.CFMetaData;
import static org.apache.cassandra.config.DatabaseDescriptor.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.locator.EndPointSnitch;
import org.apache.cassandra.locator.RackAwareStrategy;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class DefsTest extends CleanupHelper
{
    @Before
    public void setup()
    {
        // just something to ensure that DD has been initialized.
        DatabaseDescriptor.getNonSystemTables();
    }

    @Test
    public void saveAndRestore() throws IOException
    {
        // verify dump and reload.
        UUID first = UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress());
        DefsTable.dumpToStorage(first);
        List<KSMetaData> defs = new ArrayList<KSMetaData>(DefsTable.loadFromStorage(first));

        assert defs.size() > 0;
        assert defs.size() == DatabaseDescriptor.getNonSystemTables().size();
        for (KSMetaData loaded : defs)
        {
            KSMetaData defined = DatabaseDescriptor.getTableDefinition(loaded.name);
            assert defined.equals(loaded);
        }
    }
     
    @Test
    public void addNewCfToBogusTable() throws InterruptedException
    {
        CFMetaData newCf = new CFMetaData("MadeUpKeyspace", "NewCF", "Standard", new UTF8Type(), null, "new cf", 0, 0);
        try
        {
            DefsTable.add(newCf).get();
            throw new AssertionError("You should't be able to do anything to a keyspace that doesn't exist.");
        }
        catch (ExecutionException expected)
        {
        }
    }

    @Test
    public void addNewCF() throws IOException, ExecutionException, InterruptedException
    {
        final String ks = "Keyspace1";
        final String cf = "BrandNewCf";
        KSMetaData original = DatabaseDescriptor.getTableDefinition(ks);

        CFMetaData newCf = new CFMetaData(original.name, cf, "Standard", new UTF8Type(), null, "A New Column Family", 0, 0);
        int clSegments = CommitLog.instance().getSegmentCount();
        assert !DatabaseDescriptor.getTableDefinition(ks).cfMetaData().containsKey(newCf.cfName);
        DefsTable.add(newCf).get();
        assert CommitLog.instance().getSegmentCount() == clSegments + 1;

        assert DatabaseDescriptor.getTableDefinition(ks).cfMetaData().containsKey(newCf.cfName);
        assert DatabaseDescriptor.getTableDefinition(ks).cfMetaData().get(newCf.cfName).equals(newCf);

        // now read and write to it.
        RowMutation rm = new RowMutation(ks, "key0");
        rm.add(new QueryPath(cf, null, "col0".getBytes()), "value0".getBytes(), 1L);
        rm.apply();
        ColumnFamilyStore store = Table.open(ks).getColumnFamilyStore(cf);
        assert store != null;
        store.forceBlockingFlush();
        
        ColumnFamily cfam = store.getColumnFamily(QueryFilter.getNamesFilter("key0", new QueryPath(cf), "col0".getBytes()));
        assert cfam.getColumn("col0".getBytes()) != null;
        IColumn col = cfam.getColumn("col0".getBytes());
        assert Arrays.equals("value0".getBytes(), col.value());
    }

    @Test
    public void dropCf() throws IOException, ExecutionException, InterruptedException
    {
        // sanity
        final KSMetaData ks = DatabaseDescriptor.getTableDefinition("Keyspace1");
        assert ks != null;
        final CFMetaData cfm = ks.cfMetaData().get("Standard1");
        assert cfm != null;
        
        // write some data, force a flush, then verify that files exist on disk.
        RowMutation rm = new RowMutation(ks.name, "dropCf");
        for (int i = 0; i < 100; i++)
            rm.add(new QueryPath(cfm.cfName, null, ("col" + i).getBytes()), "anyvalue".getBytes(), 1L);
        rm.apply();
        ColumnFamilyStore store = Table.open(cfm.tableName).getColumnFamilyStore(cfm.cfName);
        assert store != null;
        store.forceBlockingFlush();
        store.getFlushPath();
        assert DefsTable.getFiles(cfm.tableName, cfm.cfName).size() > 0;
        
        DefsTable.drop(cfm, true).get();
        
        assert !DatabaseDescriptor.getTableDefinition(ks.name).cfMetaData().containsKey(cfm.cfName);
        
        // any write should fail.
        rm = new RowMutation(ks.name, "dropCf");
        try
        {
            rm.add(new QueryPath("Standard1", null, "col0".getBytes()), "value0".getBytes(), 1L);
            rm.apply();
            assert false : "This mutation should have failed since the CF no longer exists.";
        }
        catch (Throwable th)
        {
            assert th instanceof IllegalArgumentException;
        }
        
        // verify that the files are gone.
        assert DefsTable.getFiles(cfm.tableName, cfm.cfName).size() == 0;
    }    
    
    @Test
    public void renameCf() throws IOException, ExecutionException, InterruptedException
    {
        final KSMetaData ks = DatabaseDescriptor.getTableDefinition("Keyspace2");
        assert ks != null;
        final CFMetaData oldCfm = ks.cfMetaData().get("Standard1");
        assert oldCfm != null;
        
        // write some data, force a flush, then verify that files exist on disk.
        RowMutation rm = new RowMutation(ks.name, "key0");
        for (int i = 0; i < 100; i++)
            rm.add(new QueryPath(oldCfm.cfName, null, ("col" + i).getBytes()), "anyvalue".getBytes(), 1L);
        rm.apply();
        ColumnFamilyStore store = Table.open(oldCfm.tableName).getColumnFamilyStore(oldCfm.cfName);
        assert store != null;
        store.forceBlockingFlush();
        int fileCount = DefsTable.getFiles(oldCfm.tableName, oldCfm.cfName).size();
        assert fileCount > 0;
        
        final String newCfmName = "St4ndard1Replacement";
        DefsTable.rename(oldCfm, newCfmName).get();
        
        assert !DatabaseDescriptor.getTableDefinition(ks.name).cfMetaData().containsKey(oldCfm.cfName);
        assert DatabaseDescriptor.getTableDefinition(ks.name).cfMetaData().containsKey(newCfmName);
        
        // verify that new files are there.
        assert DefsTable.getFiles(oldCfm.tableName, newCfmName).size() == fileCount;
        
        // do some reads.
        store = Table.open(oldCfm.tableName).getColumnFamilyStore(newCfmName);
        assert store != null;
        ColumnFamily cfam = store.getColumnFamily(QueryFilter.getSliceFilter("key0", new QueryPath(newCfmName), "".getBytes(), "".getBytes(), null, false, 1000));
        assert cfam.getSortedColumns().size() == 100; // should be good enough?
        
        // do some writes
        rm = new RowMutation(ks.name, "key0");
        rm.add(new QueryPath(newCfmName, null, "col5".getBytes()), "updated".getBytes(), 2L);
        rm.apply();
        store.forceBlockingFlush();
        
        cfam = store.getColumnFamily(QueryFilter.getNamesFilter("key0", new QueryPath(newCfmName), "col5".getBytes()));
        assert cfam.getColumnCount() == 1;
        assert Arrays.equals(cfam.getColumn("col5".getBytes()).value(), "updated".getBytes());
    }
    
    @Test
    public void addNewKS() throws IOException, ExecutionException, InterruptedException
    {
        CFMetaData newCf = new CFMetaData("NewKeyspace1", "AddedStandard1", "Standard", new UTF8Type(), null, "A new cf for a new ks", 0, 0);
        KSMetaData newKs = new KSMetaData(newCf.tableName, RackAwareStrategy.class, 5, new EndPointSnitch(), newCf);
        
        int segmentCount = CommitLog.instance().getSegmentCount();
        DefsTable.add(newKs).get();
        assert CommitLog.instance().getSegmentCount() == segmentCount + 1;
        
        assert DatabaseDescriptor.getTableDefinition(newCf.tableName) != null;
        assert DatabaseDescriptor.getTableDefinition(newCf.tableName) == newKs;

        // test reads and writes.
        RowMutation rm = new RowMutation(newCf.tableName, "key0");
        rm.add(new QueryPath(newCf.cfName, null, "col0".getBytes()), "value0".getBytes(), 1L);
        rm.apply();
        ColumnFamilyStore store = Table.open(newCf.tableName).getColumnFamilyStore(newCf.cfName);
        assert store != null;
        store.forceBlockingFlush();
        
        ColumnFamily cfam = store.getColumnFamily(QueryFilter.getNamesFilter("key0", new QueryPath(newCf.cfName), "col0".getBytes()));
        assert cfam.getColumn("col0".getBytes()) != null;
        IColumn col = cfam.getColumn("col0".getBytes());
        assert Arrays.equals("value0".getBytes(), col.value());
    }
    
    @Test
    public void dropKS() throws IOException, ExecutionException, InterruptedException
    {
        // sanity
        final KSMetaData ks = DatabaseDescriptor.getTableDefinition("Keyspace1");
        assert ks != null;
        final CFMetaData cfm = ks.cfMetaData().get("Standard2");
        assert cfm != null;
        
        // write some data, force a flush, then verify that files exist on disk.
        RowMutation rm = new RowMutation(ks.name, "dropKs");
        for (int i = 0; i < 100; i++)
            rm.add(new QueryPath(cfm.cfName, null, ("col" + i).getBytes()), "anyvalue".getBytes(), 1L);
        rm.apply();
        ColumnFamilyStore store = Table.open(cfm.tableName).getColumnFamilyStore(cfm.cfName);
        assert store != null;
        store.forceBlockingFlush();
        assert DefsTable.getFiles(cfm.tableName, cfm.cfName).size() > 0;
        
        DefsTable.drop(ks, true).get();
        
        assert DatabaseDescriptor.getTableDefinition(ks.name) == null;
        
        // write should fail.
        rm = new RowMutation(ks.name, "dropKs");
        try
        {
            rm.add(new QueryPath("Standard1", null, "col0".getBytes()), "value0".getBytes(), 1L);
            rm.apply();
            throw new AssertionError("This mutation should have failed since the CF no longer exists.");
        }
        catch (Throwable th)
        {
            assert th instanceof IllegalArgumentException;
        }
        
        // reads should fail too.
        try
        {
            Table.open(ks.name);
        }
        catch (Throwable th)
        {
            // this is what has historically happened when you try to open a table that doesn't exist.
            assert th instanceof NullPointerException;
        }
    }
    
    @Test
    public void renameKs() throws IOException, ExecutionException, InterruptedException
    {
        final KSMetaData oldKs = DatabaseDescriptor.getTableDefinition("Keyspace2");
        assert oldKs != null;
        final String cfName = "Standard3";
        assert oldKs.cfMetaData().containsKey(cfName);
        assert oldKs.cfMetaData().get(cfName).tableName.equals(oldKs.name);
        
        // write some data that we hope to read back later.
        RowMutation rm = new RowMutation(oldKs.name, "renameKs");
        for (int i = 0; i < 10; i++)
            rm.add(new QueryPath(cfName, null, ("col" + i).getBytes()), "value".getBytes(), 1L);
        rm.apply();
        ColumnFamilyStore store = Table.open(oldKs.name).getColumnFamilyStore(cfName);
        assert store != null;
        store.forceBlockingFlush();
        assert DefsTable.getFiles(oldKs.name, cfName).size() > 0;
        
        final String newKsName = "RenamedKeyspace2";
        DefsTable.rename(oldKs, newKsName).get();
        KSMetaData newKs = DatabaseDescriptor.getTableDefinition(newKsName);
        
        assert DatabaseDescriptor.getTableDefinition(oldKs.name) == null;
        assert newKs != null;
        assert newKs.name.equals(newKsName);
        assert newKs.cfMetaData().containsKey(cfName);
        assert newKs.cfMetaData().get(cfName).tableName.equals(newKsName);
        assert DefsTable.getFiles(newKs.name, cfName).size() > 0;
        assert DefsTable.getFiles(oldKs.name, cfName).size() == 0;
        
        // read on old should fail.
        try
        {
            Table.open(oldKs.name);
        }
        catch (Throwable th)
        {
            assert th instanceof NullPointerException;
        }
        
        // write on old should fail.
        rm = new RowMutation(oldKs.name, "any key will do");
        try
        {
            rm.add(new QueryPath(cfName, null, "col0".getBytes()), "value0".getBytes(), 1L);
            rm.apply();
            throw new AssertionError("This mutation should have failed since the CF/Table no longer exists.");
        }
        catch (Throwable th)
        {
            assert th instanceof IllegalArgumentException;
        }
        
        // write on new should work.
        rm = new RowMutation(newKsName, "renameKs");
        rm.add(new QueryPath(cfName, null, "col0".getBytes()), "newvalue".getBytes(), 2L);
        rm.apply();
        store = Table.open(newKs.name).getColumnFamilyStore(cfName);
        assert store != null;
        store.forceBlockingFlush();
        
        // read on new should work.
        SortedSet<byte[]> cols = new TreeSet<byte[]>(new BytesType());
        cols.add("col0".getBytes());
        cols.add("col1".getBytes());
        ColumnFamily cfam = store.getColumnFamily(QueryFilter.getNamesFilter("renameKs", new QueryPath(cfName), cols));
        assert cfam.getColumnCount() == cols.size();
        // tests new write.
        assert Arrays.equals(cfam.getColumn("col0".getBytes()).value(), "newvalue".getBytes());
        // tests old write.
        assert Arrays.equals(cfam.getColumn("col1".getBytes()).value(), "value".getBytes());
    }
}
