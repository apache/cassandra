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

package org.apache.cassandra.db.repair;

import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.repair.state.ValidationState;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static java.util.Collections.singleton;
import static org.apache.cassandra.db.repair.CassandraValidationIterator.getSSTablesToValidate;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

/**
 * Tests correct sstables are returned from CompactionManager.getSSTablesForValidation
 * for consistent, legacy incremental, and full repairs
 */
public class CompactionManagerGetSSTablesForValidationTest
{
    private String ks;
    private static final String tbl = "tbl";
    private ColumnFamilyStore cfs;
    private static InetAddressAndPort coordinator;

    private static Token MT;

    private SSTableReader repaired;
    private SSTableReader unrepaired;
    private SSTableReader pendingRepair;

    private TimeUUID sessionID;
    private RepairJobDesc desc;

    @BeforeClass
    public static void setupClass() throws Exception
    {
        SchemaLoader.prepareServer();
        coordinator = InetAddressAndPort.getByName("10.0.0.1");
        MT = DatabaseDescriptor.getPartitioner().getMinimumToken();
    }

    @Before
    public void setup() throws Exception
    {
        ks = "ks_" + System.currentTimeMillis();
        TableMetadata cfm = CreateTableStatement.parse(String.format("CREATE TABLE %s.%s (k INT PRIMARY KEY, v INT)", ks, tbl), ks).build();
        SchemaLoader.createKeyspace(ks, KeyspaceParams.simple(1), cfm);
        cfs = Schema.instance.getColumnFamilyStoreInstance(cfm.id);
    }

    private void makeSSTables()
    {
        for (int i=0; i<3; i++)
        {
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES(?, ?)", ks, tbl), i, i);
            Util.flush(cfs);
        }
        Assert.assertEquals(3, cfs.getLiveSSTables().size());

    }

    private void registerRepair(boolean incremental) throws Exception
    {
        sessionID = nextTimeUUID();
        Range<Token> range = new Range<>(MT, MT);
        ActiveRepairService.instance().registerParentRepairSession(sessionID,
                                                                   coordinator,
                                                                   Lists.newArrayList(cfs),
                                                                   Sets.newHashSet(range),
                                                                   incremental,
                                                                   incremental ? System.currentTimeMillis() : ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                   true,
                                                                   PreviewKind.NONE);
        desc = new RepairJobDesc(sessionID, nextTimeUUID(), ks, tbl, singleton(range));
    }

    private void modifySSTables() throws Exception
    {
        Iterator<SSTableReader> iter = cfs.getLiveSSTables().iterator();

        repaired = iter.next();
        repaired.descriptor.getMetadataSerializer().mutateRepairMetadata(repaired.descriptor, System.currentTimeMillis(), null, false);
        repaired.reloadSSTableMetadata();

        pendingRepair = iter.next();
        pendingRepair.descriptor.getMetadataSerializer().mutateRepairMetadata(pendingRepair.descriptor, ActiveRepairService.UNREPAIRED_SSTABLE, sessionID, false);
        pendingRepair.reloadSSTableMetadata();

        unrepaired = iter.next();

        Assert.assertFalse(iter.hasNext());
    }

    @Test
    public void consistentRepair() throws Exception
    {
        makeSSTables();
        registerRepair(true);
        modifySSTables();

        // get sstables for repair
        Validator validator = new Validator(new ValidationState(Clock.Global.clock(), desc, coordinator), FBUtilities.nowInSeconds(), true, PreviewKind.NONE);
        Set<SSTableReader> sstables = Sets.newHashSet(getSSTablesToValidate(cfs, SharedContext.Global.instance, validator.desc.ranges, validator.desc.parentSessionId, validator.isIncremental));
        Assert.assertNotNull(sstables);
        Assert.assertEquals(1, sstables.size());
        Assert.assertTrue(sstables.contains(pendingRepair));
    }

    @Test
    public void legacyIncrementalRepair() throws Exception
    {
        makeSSTables();
        registerRepair(true);
        modifySSTables();

        // get sstables for repair
        Validator validator = new Validator(new ValidationState(Clock.Global.clock(), desc, coordinator), FBUtilities.nowInSeconds(), false, PreviewKind.NONE);
        Set<SSTableReader> sstables = Sets.newHashSet(getSSTablesToValidate(cfs, SharedContext.Global.instance, validator.desc.ranges, validator.desc.parentSessionId, validator.isIncremental));
        Assert.assertNotNull(sstables);
        Assert.assertEquals(2, sstables.size());
        Assert.assertTrue(sstables.contains(pendingRepair));
        Assert.assertTrue(sstables.contains(unrepaired));
    }

    @Test
    public void fullRepair() throws Exception
    {
        makeSSTables();
        registerRepair(false);
        modifySSTables();

        // get sstables for repair
        Validator validator = new Validator(new ValidationState(Clock.Global.clock(), desc, coordinator), FBUtilities.nowInSeconds(), false, PreviewKind.NONE);
        Set<SSTableReader> sstables = Sets.newHashSet(getSSTablesToValidate(cfs, SharedContext.Global.instance, validator.desc.ranges, validator.desc.parentSessionId, validator.isIncremental));
        Assert.assertNotNull(sstables);
        Assert.assertEquals(3, sstables.size());
        Assert.assertTrue(sstables.contains(pendingRepair));
        Assert.assertTrue(sstables.contains(unrepaired));
        Assert.assertTrue(sstables.contains(repaired));
    }
}
