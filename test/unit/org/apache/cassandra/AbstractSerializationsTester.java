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
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.net.MessagingService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AbstractSerializationsTester
{
    protected static final String CUR_VER = System.getProperty("cassandra.version", "4.1");
    protected static final Map<String, Integer> VERSION_MAP = new HashMap<String, Integer> ()
    {{
        put("3.0", MessagingService.VERSION_30);
        put("4.0", MessagingService.VERSION_40);
        put("4.1", MessagingService.VERSION_41);
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

    protected static FileInputStreamPlus getInput(String name) throws IOException
    {
        return getInput(CUR_VER, name);
    }

    protected static FileInputStreamPlus getInput(String version, String name) throws IOException
    {
        File f = new File("test/data/serialization/" + version + '/' + name);
        assert f.exists() : f.path();
        return new FileInputStreamPlus(f);
    }

    @SuppressWarnings("resource")
    protected static DataOutputStreamPlus getOutput(String name) throws IOException
    {
        return getOutput(CUR_VER, name);
    }

    @SuppressWarnings("resource")
    protected static DataOutputStreamPlus getOutput(String version, String name) throws IOException
    {
        File f = new File("test/data/serialization/" + version + '/' + name);
        f.parent().tryCreateDirectories();
        return new FileOutputStreamPlus(f);
    }
}
