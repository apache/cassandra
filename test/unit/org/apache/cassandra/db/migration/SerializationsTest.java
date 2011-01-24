package org.apache.cassandra.db.migration;

import org.apache.cassandra.AbstractSerializationsTester;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.io.SerDeUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.commons.codec.binary.Base64;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.UUID;

public class SerializationsTest extends AbstractSerializationsTester
{
    private static final int ksCount = 5;
    
    private void testWrite() throws IOException, ConfigurationException
    {
        for (int i = 0; i < ksCount; i++)
        {
            String tableName = "Keyspace" + (i + 1);
            KSMetaData ksm = DatabaseDescriptor.getKSMetaData(tableName);
            UUID uuid = UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress());
            DatabaseDescriptor.clearTableDefinition(ksm, uuid);
            Migration m = new AddKeyspace(ksm);
            ByteBuffer bytes = m.serialize();
            
            DataOutputStream out = getOutput("db.migration." + tableName + ".bin");
            out.writeUTF(new String(Base64.encodeBase64(bytes.array())));
            out.close();
        }
    }
    
    @Test
    public void testRead() throws IOException, ConfigurationException
    {
        if (AbstractSerializationsTester.EXECUTE_WRITES)
            testWrite();
        
        for (int i = 0; i < ksCount; i++)
        {
            String tableName = "Keyspace" + (i + 1);
            DataInputStream in = getInput("db.migration." + tableName + ".bin");
            byte[] raw = Base64.decodeBase64(in.readUTF().getBytes());
            org.apache.cassandra.db.migration.avro.Migration obj = new org.apache.cassandra.db.migration.avro.Migration();
            SerDeUtils.deserializeWithSchema(ByteBuffer.wrap(raw), obj);
            in.close();
        }
    }
}
