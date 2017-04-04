/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.net.IMessageSink;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.AbstractRepairTest;
import org.apache.cassandra.repair.consistent.LocalSessionAccessor;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;

@Ignore
public class AbstractPendingRepairTest extends AbstractRepairTest
{
    protected String ks;
    protected final String tbl = "tbl";
    protected TableMetadata cfm;
    protected ColumnFamilyStore cfs;
    protected CompactionStrategyManager csm;
    protected static ActiveRepairService ARS;

    private int nextSSTableKey = 0;

    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        ARS = ActiveRepairService.instance;
        LocalSessionAccessor.startup();

        // cutoff messaging service
        MessagingService.instance().addMessageSink(new IMessageSink()
        {
            public boolean allowOutgoingMessage(MessageOut message, int id, InetAddress to)
            {
                return false;
            }

            public boolean allowIncomingMessage(MessageIn message, int id)
            {
                return false;
            }
        });
    }

    @Before
    public void setup()
    {
        ks = "ks_" + System.currentTimeMillis();
        cfm = CreateTableStatement.parse(String.format("CREATE TABLE %s.%s (k INT PRIMARY KEY, v INT)", ks, tbl), ks).build();
        SchemaLoader.createKeyspace(ks, KeyspaceParams.simple(1), cfm);
        cfs = Schema.instance.getColumnFamilyStoreInstance(cfm.id);
        csm = cfs.getCompactionStrategyManager();
        nextSSTableKey = 0;
        cfs.disableAutoCompaction();
    }

    /**
     * creates and returns an sstable
     *
     * @param orphan if true, the sstable will be removed from the unrepaired strategy
     */
    SSTableReader makeSSTable(boolean orphan)
    {
        int pk = nextSSTableKey++;
        Set<SSTableReader> pre = cfs.getLiveSSTables();
        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES(?, ?)", ks, tbl), pk, pk);
        cfs.forceBlockingFlush();
        Set<SSTableReader> post = cfs.getLiveSSTables();
        Set<SSTableReader> diff = new HashSet<>(post);
        diff.removeAll(pre);
        assert diff.size() == 1;
        SSTableReader sstable = diff.iterator().next();
        if (orphan)
        {
            Iterables.any(csm.getUnrepaired(), s -> s.getSSTables().contains(sstable));
            csm.getUnrepaired().forEach(s -> s.removeSSTable(sstable));
        }
        return sstable;
    }

    protected static void mutateRepaired(SSTableReader sstable, long repairedAt, UUID pendingRepair)
    {
        try
        {
            sstable.descriptor.getMetadataSerializer().mutateRepaired(sstable.descriptor, repairedAt, pendingRepair);
            sstable.reloadSSTableMetadata();
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    protected static void mutateRepaired(SSTableReader sstable, long repairedAt)
    {
        mutateRepaired(sstable, repairedAt, ActiveRepairService.NO_PENDING_REPAIR);
    }

    protected static void mutateRepaired(SSTableReader sstable, UUID pendingRepair)
    {
        mutateRepaired(sstable, ActiveRepairService.UNREPAIRED_SSTABLE, pendingRepair);
    }
}
