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

import org.apache.avro.Schema;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.io.SerDeUtils;
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

        // deserialize ks
        try
        {
            ksm = KSMetaData.inflate(SerDeUtils.<org.apache.cassandra.config.avro.KsDef>deserializeWithSchema(FBUtilities.readShortByteArray(din)));
        }
        catch (ConfigurationException e)
        {
            throw new IOException(e);
        }
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
        for (CFMetaData cfm : ksm.cfMetaData().values())
        {
            try
            {
                CFMetaData.map(cfm);
            }
            catch (ConfigurationException ex)
            {
                // throw RTE since this indicates a table,cf maps to an existing ID. It shouldn't if this is really a
                // new keyspace.
                throw new RuntimeException(ex);
            }
        }
        DatabaseDescriptor.setTableDefinition(ksm, newVersion);
        // these definitions could have come from somewhere else.
        CFMetaData.fixMaxId();
        if (!clientMode)
        {
            Table.open(ksm.name);
            CommitLog.instance().forceNewSegment();
        }
    }
    
    private static final class Serializer implements ICompactSerializer<AddKeyspace>
    {
        public void serialize(AddKeyspace addKeyspace, DataOutputStream dos) throws IOException
        {
            dos.write(UUIDGen.decompose(addKeyspace.newVersion));
            dos.write(UUIDGen.decompose(addKeyspace.lastVersion));
            RowMutation.serializer().serialize(addKeyspace.rm, dos);
            // serialize the added ks
            // TODO: sloppy, but migrations should be converted to Avro soon anyway
            FBUtilities.writeShortByteArray(SerDeUtils.serializeWithSchema(addKeyspace.ksm.deflate()), dos);
        }

        public AddKeyspace deserialize(DataInputStream dis) throws IOException
        {
            return new AddKeyspace(dis);
        }
    }
}
