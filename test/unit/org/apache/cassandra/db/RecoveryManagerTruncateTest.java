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
package org.apache.cassandra.db;

import static org.apache.cassandra.Util.column;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Test for the truncate operation.
 */
public class RecoveryManagerTruncateTest
{
    private static final String KEYSPACE1 = "RecoveryManagerTruncateTest";
    private static final String CF_STANDARD1 = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

	@Test
	public void testTruncate() throws IOException
	{
		Keyspace keyspace = Keyspace.open(KEYSPACE1);
		ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard1");

		Mutation rm;
		ColumnFamily cf;

		// add a single cell
        cf = ArrayBackedSortedColumns.factory.create(KEYSPACE1, "Standard1");
		cf.addColumn(column("col1", "val1", 1L));
        rm = new Mutation(KEYSPACE1, ByteBufferUtil.bytes("keymulti"), cf);
		rm.applyUnsafe();

		// Make sure data was written
		assertNotNull(getFromTable(keyspace, "Standard1", "keymulti", "col1"));

		// and now truncate it
		cfs.truncateBlocking();
        CommitLog.instance.resetUnsafe(false);
		CommitLog.instance.recover();

		// and validate truncation.
		assertNull(getFromTable(keyspace, "Standard1", "keymulti", "col1"));
	}

	private Cell getFromTable(Keyspace keyspace, String cfName, String keyName, String columnName)
	{
		ColumnFamily cf;
		ColumnFamilyStore cfStore = keyspace.getColumnFamilyStore(cfName);
		if (cfStore == null)
		{
			return null;
		}
		cf = cfStore.getColumnFamily(Util.namesQueryFilter(cfStore, Util.dk(keyName), columnName));
		if (cf == null)
		{
			return null;
		}
		return cf.getColumn(Util.cellname(columnName));
	}
}
