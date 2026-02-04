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

package org.apache.cassandra.utils;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.junit.AfterClass;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tools.JsonTransformer;
import org.apache.cassandra.tools.Util;

public class TestHelper
{
    public static void verifyAndPrint(ColumnFamilyStore cfs, SSTableReader sstable) throws IOException
    {
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false,
                                                       IVerifier.options().invokeDiskFailurePolicy(true).extendedVerification(true).build()))
        {
            verifier.verify();
        }
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            JsonTransformer.toJsonLines(scanner, Util.iterToStream(scanner), false, false, sstable.metadata(),
                                        Clock.Global.currentTimeMillis() / 1000, System.out);
        }
    }

    @AfterClass
    public static void teardown() throws IOException, ExecutionException, InterruptedException
    {
        CommitLog.instance.shutdownBlocking();
        ClusterMetadataService.instance().log().close();
        CQLTester.tearDownClass();
        CQLTester.cleanup();
    }
}
