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

import java.io.IOException;
import java.util.Collection;

import org.junit.Assert;
import org.junit.BeforeClass;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.concurrent.AbstractTransactionalTest;

public class SSTableWriterTransactionTest extends AbstractTransactionalTest
{
    public static final String KEYSPACE1 = "BigTableWriterTest";
    public static final String CF_STANDARD = "Standard1";

    private static ColumnFamilyStore cfs;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD, 0, Int32Type.instance, AsciiType.instance, Int32Type.instance));
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
        final SSTableTxnWriter writer;

        private TestableBTW()
        {
            this(cfs.newSSTableDescriptor(cfs.getDirectories().getDirectoryForNewSSTables()));
        }

        private TestableBTW(Descriptor desc)
        {
            this(desc, SSTableTxnWriter.create(cfs, desc, 0, 0, null, false,
                                               new SerializationHeader(true, cfs.metadata(),
                                                                       cfs.metadata().regularAndStaticColumns(),
                                                                       EncodingStats.NO_STATS)));
        }

        private TestableBTW(Descriptor desc, SSTableTxnWriter sw)
        {
            super(sw);
            this.file = desc.fileFor(Components.DATA);
            this.descriptor = desc;
            this.writer = sw;

            for (int i = 0; i < 100; i++)
            {
                UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), i);
                for (int j = 0; j < 10; j++)
                    update.newRow(j).add("val", SSTableRewriterTest.random(0, 1000));
                writer.append(update.build().unfilteredIterator());
            }
        }

        protected void assertInProgress() throws Exception
        {
            assertExists(descriptor.version.format.primaryComponents());
            assertNotExists(descriptor.version.format.generatedOnLoadComponents());
            Assert.assertTrue(file.length() > 0);
        }

        protected void assertPrepared() throws Exception
        {
            assertExists(descriptor.version.format.primaryComponents());
            assertExists(descriptor.version.format.generatedOnLoadComponents());
        }

        protected void assertAborted() throws Exception
        {
            assertNotExists(descriptor.version.format.primaryComponents());
            assertNotExists(descriptor.version.format.generatedOnLoadComponents());
            Assert.assertFalse(file.exists());
        }

        protected void assertCommitted() throws Exception
        {
            assertPrepared();
        }

        @Override
        protected boolean commitCanThrow()
        {
            return true;
        }

        private void assertExists(Collection<Component> components)
        {
            for (Component component : components)
                Assert.assertTrue(descriptor.fileFor(component).exists());
        }

        private void assertNotExists(Collection<Component> components)
        {
            for (Component component : components)
                Assert.assertFalse(component.toString(), descriptor.fileFor(component).exists());
        }
    }
}
