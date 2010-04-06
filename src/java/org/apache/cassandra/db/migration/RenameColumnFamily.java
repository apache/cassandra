package org.apache.cassandra.db.migration;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DatabaseDescriptor.ConfigurationException;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.DefsTable;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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


public class RenameColumnFamily extends Migration
{
    private static final Serializer serializer = new Serializer();
    
    private String tableName;
    private String oldName;
    private String newName;
    
    RenameColumnFamily(DataInputStream din) throws IOException
    {
        super(UUIDGen.makeType1UUID(din), UUIDGen.makeType1UUID(din));
        rm = RowMutation.serializer().deserialize(din);
        tableName = din.readUTF();
        oldName = din.readUTF();
        newName = din.readUTF();
    }
    
    public RenameColumnFamily(String tableName, String oldName, String newName) throws ConfigurationException, IOException
    {
        super(UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress()), DatabaseDescriptor.getDefsVersion());
        this.tableName = tableName;
        this.oldName = oldName;
        this.newName = newName;
        
        KSMetaData ksm = DatabaseDescriptor.getTableDefinition(tableName);
        if (ksm == null)
            throw new DatabaseDescriptor.ConfigurationException("Keyspace does not already exist.");
        if (!ksm.cfMetaData().containsKey(oldName))
            throw new DatabaseDescriptor.ConfigurationException("CF is not defined in that keyspace.");
        if (ksm.cfMetaData().containsKey(newName))
            throw new DatabaseDescriptor.ConfigurationException("CF is already defined in that keyspace.");
        
        // clone the ksm, replacing cfm with the new one.
        KSMetaData newKsm = makeNewKeyspaceDefinition(ksm);
        rm = Migration.makeDefinitionMutation(newKsm, null, newVersion);
    }
    
    private KSMetaData makeNewKeyspaceDefinition(KSMetaData ksm)
    {
        CFMetaData oldCfm = ksm.cfMetaData().get(oldName);
        List<CFMetaData> newCfs = new ArrayList<CFMetaData>(ksm.cfMetaData().values());
        newCfs.remove(oldCfm);
        assert newCfs.size() == ksm.cfMetaData().size() - 1;
        CFMetaData newCfm = CFMetaData.rename(oldCfm, newName);
        newCfs.add(newCfm);
        return new KSMetaData(ksm.name, ksm.strategyClass, ksm.replicationFactor, ksm.snitch, newCfs.toArray(new CFMetaData[newCfs.size()]));
    }

    @Override
    public ICompactSerializer getSerializer()
    {
        return serializer;
    }

    @Override
    public void applyModels() throws IOException
    {
        // leave it up to operators to ensure there are no writes going on durng the file rename. Just know that
        // attempting row mutations on oldcfName right now would be really bad.
        renameCfStorageFiles(tableName, oldName, newName);
        
        // reset defs.
        KSMetaData ksm = makeNewKeyspaceDefinition(DatabaseDescriptor.getTableDefinition(tableName));
        DatabaseDescriptor.setTableDefinition(ksm, newVersion);
        Table.open(ksm.name).renameCf(oldName, newName);
        
        CommitLog.instance().forceNewSegment();
    }
    
    // if this errors out, we are in a world of hurt.
    private static void renameCfStorageFiles(String table, String oldCfName, String newCfName) throws IOException
    {
        // complete as much of the job as possible.  Don't let errors long the way prevent as much renaming as possible
        // from happening.
        IOException mostRecentProblem = null;
        for (File existing : DefsTable.getFiles(table, oldCfName))
        {
            try
            {
                String newFileName = existing.getName().replaceFirst("\\w+-", newCfName + "-");
                FileUtils.renameWithConfirm(existing, new File(existing.getParent(), newFileName));
            }
            catch (IOException ex)
            {
                mostRecentProblem = ex;
            }
        }
        if (mostRecentProblem != null)
            throw new IOException("One or more IOExceptions encountered while renaming files. Most recent problem is included.", mostRecentProblem);
    }
    
    private static final class Serializer implements ICompactSerializer<RenameColumnFamily>
    {
        public void serialize(RenameColumnFamily renameColumnFamily, DataOutputStream dos) throws IOException
        {
            dos.write(UUIDGen.decompose(renameColumnFamily.newVersion));
            dos.write(UUIDGen.decompose(renameColumnFamily.lastVersion));
            RowMutation.serializer().serialize(renameColumnFamily.rm, dos);
            
            dos.writeUTF(renameColumnFamily.tableName);
            dos.writeUTF(renameColumnFamily.oldName);
            dos.writeUTF(renameColumnFamily.newName);
        }

        public RenameColumnFamily deserialize(DataInputStream dis) throws IOException
        {
            return new RenameColumnFamily(dis);
        }
    }
}
