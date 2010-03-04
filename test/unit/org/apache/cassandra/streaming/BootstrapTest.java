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
package org.apache.cassandra.streaming;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.SSTable;

import org.junit.Test;

public class BootstrapTest
{
    @Test
    public void testGetNewNames() throws IOException
    {
        SSTable.Descriptor desc = SSTable.Descriptor.fromFilename(new File("Keyspace1", "Standard1-500-Data.db").toString());
        PendingFile[] pendingFiles = new PendingFile[]{ new PendingFile(desc, "Data.db", 100),
                                                        new PendingFile(desc, "Index.db", 100),
                                                        new PendingFile(desc, "Filter.db", 100) };
        StreamInitiateVerbHandler bivh = new StreamInitiateVerbHandler();

        // map the input (remote) contexts to output (local) contexts
        Map<PendingFile, PendingFile> mapping = bivh.getContextMapping(pendingFiles);
        assertEquals(pendingFiles.length, mapping.size());
        for (PendingFile inContext : pendingFiles)
        {
            PendingFile outContext = mapping.get(inContext);
            // filename and generation are expected to have changed
            assert !inContext.getFilename().equals(outContext.getFilename());

            // nothing else should
            assertEquals(inContext.getComponent(), outContext.getComponent());
            assertEquals(inContext.getDescriptor().ksname, outContext.getDescriptor().ksname);
            assertEquals(inContext.getDescriptor().cfname, outContext.getDescriptor().cfname);
        }
    }
}
