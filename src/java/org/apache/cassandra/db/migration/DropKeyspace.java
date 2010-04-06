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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DatabaseDescriptor.ConfigurationException;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.HintedHandOffManager;
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

public class DropKeyspace extends Migration
{
    private static final Serializer serializer = new Serializer();
    
    private String name;
    private boolean blockOnFileDeletion;
    
    private DropKeyspace(DataInputStream din) throws IOException
    {
        super(UUIDGen.makeType1UUID(din), UUIDGen.makeType1UUID(din));
        rm = RowMutation.serializer().deserialize(din);
        name = din.readUTF();
    }
    
    public DropKeyspace(String name, boolean blockOnFileDeletion) throws ConfigurationException, IOException
    {
        super(UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress()), DatabaseDescriptor.getDefsVersion());
        this.name = name;
        this.blockOnFileDeletion = blockOnFileDeletion;
        KSMetaData ksm = DatabaseDescriptor.getTableDefinition(name);
        if (ksm == null)
            throw new ConfigurationException("Keyspace does not exist.");
        rm = makeDefinitionMutation(null, ksm, newVersion);
    }

    @Override
    public ICompactSerializer getSerializer()
    {
        return serializer;
    }

    @Override
    public void applyModels() throws IOException
    {
        KSMetaData ksm = DatabaseDescriptor.getTableDefinition(name);
        // remove the table from the static instances.
        Table table = Table.clear(ksm.name);
        if (table == null)
            throw new IOException("Table is not active. " + ksm.name);
        
        // remove all cfs from the table instance.
        for (CFMetaData cfm : ksm.cfMetaData().values())
        {
            CFMetaData.purge(cfm);
            table.dropCf(cfm.cfName);
            SystemTable.markForRemoval(cfm);
        }
                        
        // reset defs.
        DatabaseDescriptor.clearTableDefinition(ksm, newVersion);
        CommitLog.instance().forceNewSegment();
        Migration.cleanupDeadFiles(blockOnFileDeletion);
        
        // clear up any local hinted data for this keyspace.
        HintedHandOffManager.renameHints(name, null);
    }
    
    private static final class Serializer implements ICompactSerializer<DropKeyspace>
    {
        public void serialize(DropKeyspace dropKeyspace, DataOutputStream dos) throws IOException
        {
            dos.write(UUIDGen.decompose(dropKeyspace.newVersion));
            dos.write(UUIDGen.decompose(dropKeyspace.lastVersion));
            RowMutation.serializer().serialize(dropKeyspace.rm, dos);
            
            dos.writeUTF(dropKeyspace.name);
        }

        public DropKeyspace deserialize(DataInputStream dis) throws IOException
        {
            return new DropKeyspace(dis);
        }
    }
}
