package org.apache.cassandra;
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


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AbstractSerializationsTester extends CleanupHelper
{
    protected static final String CUR_VER = System.getProperty("cassandra.version", "0.7");
    protected static final Map<String, Integer> VERSION_MAP = new HashMap<String, Integer> () 
    {{
        put("0.7", 1);
    }};
    
    protected static final boolean EXECUTE_WRITES = new Boolean(System.getProperty("cassandra.test-serialization-writes", "False")).booleanValue();
    
    protected final int getVersion()
    {
        return VERSION_MAP.get(CUR_VER);
    }
    
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
