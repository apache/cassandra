/*
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
package org.apache.cassandra;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputStreamAndChannel;
import org.apache.cassandra.net.MessagingService;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AbstractSerializationsTester
{
    protected static final String CUR_VER = System.getProperty("cassandra.version", "2.1");
    protected static final Map<String, Integer> VERSION_MAP = new HashMap<String, Integer> ()
    {{
        put("0.7", 1);
        put("1.0", 3);
        put("1.2", MessagingService.VERSION_12);
        put("2.0", MessagingService.VERSION_20);
        put("2.1", MessagingService.VERSION_21);
    }};

    protected static final boolean EXECUTE_WRITES = Boolean.getBoolean("cassandra.test-serialization-writes");

    protected static int getVersion()
    {
        return VERSION_MAP.get(CUR_VER);
    }

    protected <T> void testSerializedSize(T obj, IVersionedSerializer<T> serializer) throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer();
        serializer.serialize(obj, out, getVersion());
        assert out.getLength() == serializer.serializedSize(obj, getVersion());
    }

    protected static DataInputStream getInput(String name) throws IOException
    {
        File f = new File("test/data/serialization/" + CUR_VER + "/" + name);
        assert f.exists() : f.getPath();
        return new DataInputStream(new FileInputStream(f));
    }

    protected static DataOutputStreamAndChannel getOutput(String name) throws IOException
    {
        File f = new File("test/data/serialization/" + CUR_VER + "/" + name);
        f.getParentFile().mkdirs();
        return new DataOutputStreamAndChannel(new FileOutputStream(f));
    }
}
