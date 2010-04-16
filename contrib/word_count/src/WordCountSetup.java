/**
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

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ConsistencyLevel;

public class WordCountSetup
{
    private static final Logger logger = LoggerFactory.getLogger(WordCountSetup.class);

    public static final int TEST_COUNT = 4;

    public static void main(String[] args) throws Exception
    {
        StorageService.instance.initClient();
        logger.info("Sleeping " + WordCount.RING_DELAY);
        Thread.sleep(WordCount.RING_DELAY);
        assert !StorageService.instance.getLiveNodes().isEmpty();

        RowMutation rm;
        ColumnFamily cf;
        byte[] columnName;

        // text0: no rows

        // text1: 1 row, 1 word
        columnName = "text1".getBytes();
        rm = new RowMutation(WordCount.KEYSPACE, "Key0".getBytes());
        cf = ColumnFamily.create(WordCount.KEYSPACE, WordCount.COLUMN_FAMILY);
        cf.addColumn(new Column(columnName, "word1".getBytes(), 0));
        rm.add(cf);
        StorageProxy.mutateBlocking(Arrays.asList(rm), ConsistencyLevel.ONE);
        logger.info("added text1");

        // text2: 1 row, 2 words
        columnName = "text2".getBytes();
        rm = new RowMutation(WordCount.KEYSPACE, "Key0".getBytes());
        cf = ColumnFamily.create(WordCount.KEYSPACE, WordCount.COLUMN_FAMILY);
        cf.addColumn(new Column(columnName, "word1 word2".getBytes(), 0));
        rm.add(cf);
        StorageProxy.mutateBlocking(Arrays.asList(rm), ConsistencyLevel.ONE);
        logger.info("added text2");

        // text3: 1000 rows, 1 word
        columnName = "text3".getBytes();
        for (int i = 0; i < 1000; i++)
        {
            rm = new RowMutation(WordCount.KEYSPACE, ("Key" + i).getBytes());
            cf = ColumnFamily.create(WordCount.KEYSPACE, WordCount.COLUMN_FAMILY);
            cf.addColumn(new Column(columnName, "word1".getBytes(), 0));
            rm.add(cf);
            StorageProxy.mutateBlocking(Arrays.asList(rm), ConsistencyLevel.ONE);
        }
        logger.info("added text3");

        System.exit(0);
    }
}
