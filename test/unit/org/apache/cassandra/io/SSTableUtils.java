/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.io;

import java.io.File;
import java.io.IOException;

import java.util.Map;
import java.util.SortedMap;

import org.apache.cassandra.dht.IPartitioner;

public class SSTableUtils
{
    public static File tempSSTableFileName(String cfname) throws IOException
    {
        return File.createTempFile(cfname + "-", "-" + SSTable.TEMPFILE_MARKER + "-Data.db");
    }

    public static SSTableReader writeSSTable(String cfname, SortedMap<String, byte[]> entries, int expectedKeys, IPartitioner partitioner, double cacheFraction) throws IOException
    {
        File f = tempSSTableFileName(cfname);
        SSTableWriter writer = new SSTableWriter(f.getAbsolutePath(), expectedKeys, partitioner);
        for (Map.Entry<String, byte[]> entry : entries.entrySet())
        {
            writer.append(writer.partitioner.decorateKey(entry.getKey()), entry.getValue());
        }
        return writer.closeAndOpenReader(cacheFraction);
    }
}
