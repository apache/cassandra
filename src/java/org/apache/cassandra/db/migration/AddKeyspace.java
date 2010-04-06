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
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class AddKeyspace extends Migration
{
    private static final Serializer serializer = new Serializer();
    
    private KSMetaData ksm;
    
    private AddKeyspace(DataInputStream din) throws IOException
    {
        super(UUIDGen.makeType1UUID(din), UUIDGen.makeType1UUID(din));
        rm = RowMutation.serializer().deserialize(din);
        ksm = KSMetaData.deserialize(din);
    }
    
    public AddKeyspace(KSMetaData ksm) throws ConfigurationException, IOException
    {
        super(UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress()), DatabaseDescriptor.getDefsVersion());
        
        if (DatabaseDescriptor.getTableDefinition(ksm.name) != null)
            throw new ConfigurationException("Keyspace already exists.");
        
        this.ksm = ksm;
        rm = makeDefinitionMutation(ksm, null, newVersion);
    }

    @Override
    public ICompactSerializer getSerializer()
    {
        return serializer;
    }

    @Override
    public void applyModels() throws IOException
    {
        DatabaseDescriptor.setTableDefinition(ksm, newVersion);
        // these definitions could have come from somewhere else.
        CFMetaData.fixMaxId();
        Table.open(ksm.name);
        CommitLog.instance().forceNewSegment();
    }
    
    private static final class Serializer implements ICompactSerializer<AddKeyspace>
    {
        public void serialize(AddKeyspace addKeyspace, DataOutputStream dos) throws IOException
        {
            dos.write(UUIDGen.decompose(addKeyspace.newVersion));
            dos.write(UUIDGen.decompose(addKeyspace.lastVersion));
            RowMutation.serializer().serialize(addKeyspace.rm, dos);
            dos.write(KSMetaData.serialize(addKeyspace.ksm));
        }

        public AddKeyspace deserialize(DataInputStream dis) throws IOException
        {
            return new AddKeyspace(dis);
        }
    }
}
