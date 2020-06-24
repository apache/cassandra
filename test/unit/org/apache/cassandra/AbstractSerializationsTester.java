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
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.net.MessagingService;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AbstractSerializationsTester
{
    protected static final String CUR_VER = System.getProperty("cassandra.version", "4.0");
    protected static final Map<String, Integer> VERSION_MAP = new HashMap<String, Integer> ()
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-1949
    {{
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5220
        put("3.0", MessagingService.VERSION_30);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13257
        put("4.0", MessagingService.VERSION_40);
    }};

    protected static final boolean EXECUTE_WRITES = Boolean.getBoolean("cassandra.test-serialization-writes");

    protected static int getVersion()
    {
        return VERSION_MAP.get(CUR_VER);
    }

    protected <T> void testSerializedSize(T obj, IVersionedSerializer<T> serializer) throws IOException
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6781
        DataOutputBuffer out = new DataOutputBuffer();
        serializer.serialize(obj, out, getVersion());
        assert out.getLength() == serializer.serializedSize(obj, getVersion());
    }

    protected static DataInputStreamPlus getInput(String name) throws IOException
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8413
        return getInput(CUR_VER, name);
    }

    protected static DataInputStreamPlus getInput(String version, String name) throws IOException
    {
        File f = new File("test/data/serialization/" + version + '/' + name);
        assert f.exists() : f.getPath();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9499
        return new DataInputPlus.DataInputStreamPlus(new FileInputStream(f));
    }

    @SuppressWarnings("resource")
    protected static DataOutputStreamPlus getOutput(String name) throws IOException
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8413
        return getOutput(CUR_VER, name);
    }

    @SuppressWarnings("resource")
    protected static DataOutputStreamPlus getOutput(String version, String name) throws IOException
    {
        File f = new File("test/data/serialization/" + version + '/' + name);
        f.getParentFile().mkdirs();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8670
        return new BufferedDataOutputStreamPlus(new FileOutputStream(f).getChannel());
    }
}
