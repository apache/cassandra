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

package org.apache.cassandra.db.migration;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DatabaseDescriptor.ConfigurationException;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.DefsTable;
import org.apache.cassandra.db.HintedHandOffManager;
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
import java.util.Set;

public class RenameKeyspace extends Migration
{
    private static final Serializer serializer = new Serializer();
    
    private String oldName;
    private String newName;
    
    RenameKeyspace(DataInputStream din) throws IOException
    {
        super(UUIDGen.makeType1UUID(din), UUIDGen.makeType1UUID(din));
        rm = RowMutation.serializer().deserialize(din);
        oldName = din.readUTF();
        newName = din.readUTF();
    }
    
    public RenameKeyspace(String oldName, String newName) throws ConfigurationException, IOException
    {
        super(UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress()), DatabaseDescriptor.getDefsVersion());
        this.oldName = oldName;
        this.newName = newName;
        
        KSMetaData oldKsm = DatabaseDescriptor.getTableDefinition(oldName);
        if (oldKsm == null)
            throw new ConfigurationException("Keyspace either does not exist or does not match the one currently defined.");
        if (DatabaseDescriptor.getTableDefinition(newName) != null)
            throw new ConfigurationException("Keyspace already exists.");
        
        // clone the ksm, replacing thename.
        KSMetaData newKsm = KSMetaData.rename(oldKsm, newName, false); 
        
        rm = makeDefinitionMutation(newKsm, oldKsm, newVersion);
    }

    @Override
    public ICompactSerializer getSerializer()
    {
        return serializer;
    }

    @Override
    public void applyModels() throws IOException
    {
        renameKsStorageFiles(oldName, newName);
        
        KSMetaData oldKsm = DatabaseDescriptor.getTableDefinition(oldName);
        KSMetaData newKsm = KSMetaData.rename(oldKsm, newName, true);
        // ^^ at this point, the static methods in CFMetaData will start returning references to the new table, so
        // it helps if the node is reasonably quiescent with respect to this ks.
        DatabaseDescriptor.clearTableDefinition(oldKsm, newVersion);
        DatabaseDescriptor.setTableDefinition(newKsm, newVersion);
        Table.clear(oldKsm.name);
        Table.open(newName);
        // this isn't strictly necessary since the set of all cfs was not modified.
        CommitLog.instance().forceNewSegment();

        HintedHandOffManager.renameHints(oldName, newName);
    }
    
    private static void renameKsStorageFiles(String oldKs, String newKs) throws IOException
    {
        IOException mostRecentProblem = null;
        Set<String> cfNames = DatabaseDescriptor.getTableDefinition(oldKs).cfMetaData().keySet();
        for (String cfName : cfNames)
        {
            for (File existing : DefsTable.getFiles(oldKs, cfName))
            {
                try
                {
                    File newParent = new File(existing.getParentFile().getParent(), newKs);
                    newParent.mkdirs();
                    FileUtils.renameWithConfirm(existing, new File(newParent, existing.getName()));
                }
                catch (IOException ex)
                {
                    mostRecentProblem = ex;
                }
            }
        }
        if (mostRecentProblem != null)
            throw new IOException("One or more IOExceptions encountered while renaming files. Most recent problem is included.", mostRecentProblem);
    }
    
    private static final class Serializer implements ICompactSerializer<RenameKeyspace>
    {
        public void serialize(RenameKeyspace renameKeyspace, DataOutputStream dout) throws IOException
        {
            dout.write(UUIDGen.decompose(renameKeyspace.newVersion));
            dout.write(UUIDGen.decompose(renameKeyspace.lastVersion));
            RowMutation.serializer().serialize(renameKeyspace.rm, dout);
            
            dout.writeUTF(renameKeyspace.oldName);
            dout.writeUTF(renameKeyspace.newName);
        }

        public RenameKeyspace deserialize(DataInputStream dis) throws IOException
        {
            return new RenameKeyspace(dis);
        }
    }
}
