package org.apache.cassandra;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class AbstractSerializationsTester extends SchemaLoader
{
    protected static final String CUR_VER = System.getProperty("cassandra.version", "0.7");
    
    protected static final boolean EXECUTE_WRITES = new Boolean(System.getProperty("cassandra.test-serialization-writes", "False")).booleanValue();
    
    protected static DataInputStream getInput(String name) throws IOException
    {
        File f = new File("test/data/serialization/" + CUR_VER + "/" + name);
        assert f.exists() : f.getPath();
        return new DataInputStream(new FileInputStream(f));
    }
    
    protected static DataOutputStream getOutput(String name) throws IOException
    {
        File f = new File("test/data/serialization/" + CUR_VER + "/" + name);
        f.getParentFile().mkdirs();
        return new DataOutputStream(new FileOutputStream(f));
    }
}
