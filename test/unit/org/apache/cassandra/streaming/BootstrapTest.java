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

import java.io.File;
import java.io.IOException;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.Pair;

import java.util.Arrays;

import org.junit.Test;

public class BootstrapTest extends SchemaLoader
{
    @Test
    public void testGetNewNames() throws IOException
    {
        Descriptor desc = Descriptor.fromFilename(new File("Keyspace1", "Standard1-f-500-Data.db").toString());
        assert !desc.isLatestVersion; // deliberately test old version
        PendingFile inContext = new PendingFile(null, desc, "Data.db", Arrays.asList(new Pair<Long,Long>(0L, 1L)), OperationType.BOOTSTRAP);

        PendingFile outContext = StreamIn.getContextMapping(inContext);
        // filename and generation are expected to have changed
        assert !inContext.getFilename().equals(outContext.getFilename());

        // nothing else should
        assertEquals(inContext.component, outContext.component);
        assertEquals(desc.ksname, outContext.desc.ksname);
        assertEquals(desc.cfname, outContext.desc.cfname);
        assertEquals(desc.version, outContext.desc.version);
    }
}
