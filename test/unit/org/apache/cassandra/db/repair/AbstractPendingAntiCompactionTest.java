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

import java.util.Collection;
import java.util.Collections;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.AbstractRepairTest;
import org.apache.cassandra.repair.consistent.LocalSessionAccessor;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.TimeUUID;

@Ignore
public abstract class AbstractPendingAntiCompactionTest
{

    static final Collection<Range<Token>> FULL_RANGE;
    static final Collection<Range<Token>> NO_RANGES = Collections.emptyList();
    static InetAddressAndPort local;

    static
    {
        DatabaseDescriptor.daemonInitialization();
        Token minToken = DatabaseDescriptor.getPartitioner().getMinimumToken();
        FULL_RANGE = Collections.singleton(new Range<>(minToken, minToken));
    }

    String ks;
    final String tbl = "tbl";
    final String tbl2 = "tbl2";

    TableMetadata cfm;
    ColumnFamilyStore cfs;
    ColumnFamilyStore cfs2;

    @BeforeClass
    public static void setupClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        local = InetAddressAndPort.getByName("127.0.0.1");
        ActiveRepairService.instance().consistent.local.start();
    }

    @Before
    public void setup()
    {
        ks = "ks_" + System.currentTimeMillis();
        cfm = CreateTableStatement.parse(String.format("CREATE TABLE %s.%s (k INT PRIMARY KEY, v INT)", ks, tbl), ks).build();

        Indexes.Builder indexes = Indexes.builder();
        indexes.add(IndexMetadata.fromIndexTargets(Collections.singletonList(new IndexTarget(new ColumnIdentifier("v", true),
                                                                                             IndexTarget.Type.VALUES)),
                                                   tbl2 + "_idx",
                                                   IndexMetadata.Kind.COMPOSITES, Collections.emptyMap()));

        TableMetadata cfm2 = CreateTableStatement.parse(String.format("CREATE TABLE %s.%s (k INT PRIMARY KEY, v INT)", ks, tbl2), ks).indexes(indexes.build()).build();

        SchemaLoader.createKeyspace(ks, KeyspaceParams.simple(1), cfm, cfm2);
        cfs = Schema.instance.getColumnFamilyStoreInstance(cfm.id);
        cfs2 = Schema.instance.getColumnFamilyStoreInstance(cfm2.id);
    }

    void makeSSTables(int num)
    {
        makeSSTables(num, cfs, 2);
    }

    void makeSSTables(int num, ColumnFamilyStore cfs, int rowsPerSSTable)
    {
        for (int i = 0; i < num; i++)
        {
            int val = i * rowsPerSSTable;  // multiplied to prevent ranges from overlapping
            for (int j = 0; j < rowsPerSSTable; j++)
                QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?)", ks, cfs.getTableName()), val + j, val + j);
            Util.flush(cfs);
        }
        Assert.assertEquals(num, cfs.getLiveSSTables().size());
    }

    TimeUUID prepareSession()
    {
        TimeUUID sessionID = AbstractRepairTest.registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(sessionID, AbstractRepairTest.COORDINATOR, Sets.newHashSet(AbstractRepairTest.COORDINATOR));
        return sessionID;
    }

}
