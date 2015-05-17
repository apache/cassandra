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
package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.BeforeClass;

import junit.framework.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.AbstractTransactionalTest;

public class BigTableWriterTest extends AbstractTransactionalTest
{
    public static final String KEYSPACE1 = "BigTableWriterTest";
    public static final String CF_STANDARD = "Standard1";

    private static ColumnFamilyStore cfs;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD));
        cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD);
    }

    protected TestableTransaction newTest() throws IOException
    {
        return new TestableBTW();
    }

    private static class TestableBTW extends TestableTransaction
    {
        final File file;
        final Descriptor descriptor;
        final SSTableWriter writer;

        private TestableBTW() throws IOException
        {
            this(cfs.getTempSSTablePath(cfs.directories.getDirectoryForNewSSTables()));
        }

        private TestableBTW(String file) throws IOException
        {
            this(file, SSTableWriter.create(file, 0, 0));
        }

        private TestableBTW(String file, SSTableWriter sw) throws IOException
        {
            super(sw);
            this.file = new File(file);
            this.descriptor = Descriptor.fromFilename(file);
            this.writer = sw;
            ArrayBackedSortedColumns cf = ArrayBackedSortedColumns.factory.create(cfs.metadata);
            for (int i = 0; i < 10; i++)
                cf.addColumn(Util.cellname(i), SSTableRewriterTest.random(0, 1000), 1);
            for (int i = 0; i < 100; i++)
                writer.append(StorageService.getPartitioner().decorateKey(ByteBufferUtil.bytes(i)), cf);
        }

        protected void assertInProgress() throws Exception
        {
            assertExists(Descriptor.Type.TEMP, Component.DATA, Component.PRIMARY_INDEX);
            assertNotExists(Descriptor.Type.TEMP, Component.FILTER, Component.SUMMARY);
            assertNotExists(Descriptor.Type.FINAL, Component.DATA, Component.PRIMARY_INDEX, Component.FILTER, Component.SUMMARY);
            Assert.assertTrue(file.length() > 0);
        }

        protected void assertPrepared() throws Exception
        {
            assertNotExists(Descriptor.Type.TEMP, Component.DATA, Component.PRIMARY_INDEX, Component.FILTER, Component.SUMMARY);
            assertExists(Descriptor.Type.FINAL, Component.DATA, Component.PRIMARY_INDEX, Component.FILTER, Component.SUMMARY);
        }

        protected void assertAborted() throws Exception
        {
            assertNotExists(Descriptor.Type.TEMP, Component.DATA, Component.PRIMARY_INDEX, Component.FILTER, Component.SUMMARY);
            assertNotExists(Descriptor.Type.FINAL, Component.DATA, Component.PRIMARY_INDEX, Component.FILTER, Component.SUMMARY);
            Assert.assertFalse(file.exists());
        }

        protected void assertCommitted() throws Exception
        {
            assertPrepared();
        }

        private void assertExists(Descriptor.Type type, Component ... components)
        {
            for (Component component : components)
                Assert.assertTrue(new File(descriptor.asType(type).filenameFor(component)).exists());
        }
        private void assertNotExists(Descriptor.Type type, Component ... components)
        {
            for (Component component : components)
                Assert.assertFalse(type.toString() + " " + component.toString(), new File(descriptor.asType(type).filenameFor(component)).exists());
        }
    }

}
