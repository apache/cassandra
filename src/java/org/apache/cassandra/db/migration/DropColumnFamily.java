package org.apache.cassandra.db.migration;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

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


public class DropColumnFamily extends Migration
{
    private static final Serializer serializer = new Serializer();
    
    private String tableName;
    private String cfName;
    private boolean blockOnFileDeletion;
    
    private DropColumnFamily(DataInputStream din) throws IOException
    {
        super(UUIDGen.makeType1UUID(din), UUIDGen.makeType1UUID(din));
        rm = RowMutation.serializer().deserialize(din);
        tableName = din.readUTF();
        cfName = din.readUTF();
        blockOnFileDeletion = din.readBoolean();
    }
    
    public DropColumnFamily(String tableName, String cfName, boolean blockOnFileDeletion) throws ConfigurationException, IOException
    {
        super(UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress()), DatabaseDescriptor.getDefsVersion());
        this.tableName = tableName;
        this.cfName = cfName;
        this.blockOnFileDeletion = blockOnFileDeletion;
        
        KSMetaData ksm = DatabaseDescriptor.getTableDefinition(tableName);
        if (ksm == null)
            throw new ConfigurationException("Keyspace does not already exist.");
        else if (!ksm.cfMetaData().containsKey(cfName))
            throw new ConfigurationException("CF is not defined in that keyspace.");
        
        KSMetaData newKsm = ksm.withoutColumnFamily(cfName);
        rm = Migration.makeDefinitionMutation(newKsm, null, newVersion);
    }

    @Override
    public void beforeApplyModels()
    {
        if (clientMode)
            return;
        ColumnFamilyStore cfs = Table.open(tableName).getColumnFamilyStore(cfName);
        cfs.snapshot(Table.getTimestampedSnapshotName(null));
    }

    @Override
    public ICompactSerializer getSerializer()
    {
        return serializer;
    }

    @Override
    public void applyModels() throws IOException
    {
        // reinitialize the table.
        KSMetaData existing = DatabaseDescriptor.getTableDefinition(tableName);
        CFMetaData cfm = existing.cfMetaData().get(cfName);
        KSMetaData ksm = existing.withoutColumnFamily(cfName);
        CFMetaData.purge(cfm);
        DatabaseDescriptor.setTableDefinition(ksm, newVersion);
        
        if (!clientMode)
        {
            Table.open(ksm.name).dropCf(cfm.cfId);
            
            // indicate that some files need to be deleted (eventually)
            SystemTable.markForRemoval(cfm);
            
            // we don't really need a new segment, but let's force it to be consistent with other operations.
            CommitLog.instance().forceNewSegment();
    
            Migration.cleanupDeadFiles(blockOnFileDeletion);
        }
    }
    
    private static final class Serializer implements ICompactSerializer<DropColumnFamily>
    {
        public void serialize(DropColumnFamily dropColumnFamily, DataOutputStream dos) throws IOException
        {
            dos.write(UUIDGen.decompose(dropColumnFamily.newVersion));
            dos.write(UUIDGen.decompose(dropColumnFamily.lastVersion));
            RowMutation.serializer().serialize(dropColumnFamily.rm, dos);
            dos.writeUTF(dropColumnFamily.tableName);
            dos.writeUTF(dropColumnFamily.cfName);
            dos.writeBoolean(dropColumnFamily.blockOnFileDeletion);       
        }

        public DropColumnFamily deserialize(DataInputStream dis) throws IOException
        {
            return new DropColumnFamily(dis);
        }
    }
}
