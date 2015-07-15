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

package org.apache.cassandra.service;


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;

import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class ResponseResolverTest extends SchemaLoader
{
    private final static String KEYSPACE = "Keyspace1";
    private final static String TABLE = "Standard1";
    private final static int MAX_RESPONSE_COUNT = 3;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(MAX_RESPONSE_COUNT),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE));
    }
    
    @Test
    public void testSingleMessage_RowDigestResolver() throws DigestMismatchException, UnknownHostException
    {
        ByteBuffer key = bytes("key");
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, TABLE);
        cf.addColumn(column("c1", "v1", 0));
        Row row = new Row(key, cf);

        testReadResponses(new RowDigestResolver(KEYSPACE, key, MAX_RESPONSE_COUNT), row, makeReadResponse("127.0.0.1", row));
    }

    @Test
    public void testMultipleMessages_RowDigestResolver() throws DigestMismatchException, UnknownHostException
    {
        ByteBuffer key = bytes("key");
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, TABLE);
        cf.addColumn(column("c1", "v1", 0));
        Row row = new Row(key, cf);

        testReadResponses(new RowDigestResolver(KEYSPACE, key, MAX_RESPONSE_COUNT),
                          row,
                          makeReadResponse("127.0.0.1", row),
                          makeReadResponse("127.0.0.2", row),
                          makeReadResponse("127.0.0.3", row));
    }

    @Test(expected = DigestMismatchException.class)
    public void testDigestMismatch() throws DigestMismatchException, UnknownHostException
    {
        ByteBuffer key = bytes("key");
        ColumnFamily cf1 = ArrayBackedSortedColumns.factory.create(KEYSPACE, TABLE);
        cf1.addColumn(column("c1", "v1", 0));
        Row row1 = new Row(key, cf1);

        ColumnFamily cf2 = ArrayBackedSortedColumns.factory.create(KEYSPACE, TABLE);
        cf2.addColumn(column("c1", "v2", 1));
        Row row2 = new Row(key, cf2);

        testReadResponses(new RowDigestResolver(KEYSPACE, key, MAX_RESPONSE_COUNT),
                          row1,
                          makeReadResponse("127.0.0.1", row1),
                          makeReadResponse("127.0.0.2", row2),
                          makeReadResponse("127.0.0.3", row1));
    }

    @Test
    public void testMultipleThreads_RowDigestResolver() throws DigestMismatchException, UnknownHostException, InterruptedException
    {
        ByteBuffer key = bytes("key");
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, TABLE);
        cf.addColumn(column("c1", "v1", 0));
        Row row = new Row(key, cf);

        testReadResponsesMT(new RowDigestResolver(KEYSPACE, key, MAX_RESPONSE_COUNT),
                            row,
                            makeReadResponse("127.0.0.1", row),
                            makeReadResponse("127.0.0.2", row),
                            makeReadResponse("127.0.0.3", row));
    }

    @Test
    public void testSingleMessage_RowDataResolver() throws DigestMismatchException, UnknownHostException
    {
        ByteBuffer key = bytes("key");
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, TABLE);
        cf.addColumn(column("c1", "v1", 0));
        Row row = new Row(key, cf);

        testReadResponses(new RowDataResolver(KEYSPACE,
                                              key,
                                              new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, 10),
                                              System.currentTimeMillis(),
                                              MAX_RESPONSE_COUNT),
                          row,
                          makeReadResponse("127.0.0.1", row));
    }

    @Test
    public void testMultipleMessages_RowDataResolver() throws DigestMismatchException, UnknownHostException
    {
        ByteBuffer key = bytes("key");
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, TABLE);
        cf.addColumn(column("c1", "v1", 0));
        Row row = new Row(key, cf);

        testReadResponses(new RowDataResolver(KEYSPACE,
                                              key,
                                              new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, 10),
                                              System.currentTimeMillis(),
                                              MAX_RESPONSE_COUNT),
                          row,
                          makeReadResponse("127.0.0.1", row),
                          makeReadResponse("127.0.0.2", row),
                          makeReadResponse("127.0.0.3", row));
    }

    @Test
    public void testMultipleThreads_RowDataResolver() throws DigestMismatchException, UnknownHostException, InterruptedException
    {
        ByteBuffer key = bytes("key");
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, TABLE);
        cf.addColumn(column("c1", "v1", 0));
        Row row = new Row(key, cf);

        testReadResponsesMT(new RowDataResolver(KEYSPACE,
                                                key,
                                                new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, 10),
                                                System.currentTimeMillis(),
                                                MAX_RESPONSE_COUNT),
                            row,
                            makeReadResponse("127.0.0.1", row),
                            makeReadResponse("127.0.0.2", row),
                            makeReadResponse("127.0.0.3", row));
    }

    @Test
    public void testSingleMessage_RangeSliceResolver() throws DigestMismatchException, UnknownHostException
    {
        ByteBuffer key = bytes("key");
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, TABLE);
        cf.addColumn(column("c1", "v1", 0));

        Row[] expected = new Row[2];
        for (int i = 0; i < expected.length; i++)
            expected[i] = new Row(key, cf);

        MessageIn<RangeSliceReply> message = makeRangeSlice("127.0.0.1", expected);

        RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(KEYSPACE, System.currentTimeMillis());
        resolver.setSources(Collections.singletonList(message.from));

        testRangeSlices(resolver, expected, message);
    }

    @Test
    public void testMultipleMessages_RangeSliceResolver() throws DigestMismatchException, UnknownHostException
    {
        ByteBuffer key = bytes("key");
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, TABLE);
        cf.addColumn(column("c1", "v1", 0));

        Row[] expected = new Row[2];
        for (int i = 0; i < expected.length; i++)
            expected[i] = new Row(key, cf);

        List<InetAddress> sources = new ArrayList<>(3);
        sources.add(InetAddress.getByName("127.0.0.1"));
        sources.add(InetAddress.getByName("127.0.0.2"));
        sources.add(InetAddress.getByName("127.0.0.3"));

        RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(KEYSPACE, System.currentTimeMillis());
        resolver.setSources(sources);

        testRangeSlices(resolver,
                        expected,
                        makeRangeSlice("127.0.0.1", expected),
                        makeRangeSlice("127.0.0.2", expected),
                        makeRangeSlice("127.0.0.3", expected));
    }

    private void testReadResponses(AbstractRowResolver resolver, Row expected, MessageIn<ReadResponse> ... messages) throws DigestMismatchException
    {
        for (MessageIn<ReadResponse> message : messages)
        {
            resolver.preprocess(message);

            Row row = resolver.getData();
            checkSame(expected, row);

            row = resolver.resolve();
            checkSame(expected, row);
        }
    }

    private void testReadResponsesMT(final AbstractRowResolver resolver,
                                     final Row expected,
                                     final MessageIn<ReadResponse> ... messages) throws InterruptedException
    {
        for (MessageIn<ReadResponse> message : messages)
            resolver.preprocess(message);

        final int threadCount = 45;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch finished = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++)
        {
            executorService.submit(new Runnable()
            {
                public void run()
                {
                    try
                    {
                        Row row = resolver.getData();
                        checkSame(expected, row);

                        row = resolver.resolve();
                        checkSame(expected, row);
                    }
                    catch (DigestMismatchException ex)
                    {
                        fail(ex.getMessage());
                    }
                    finally
                    {
                        finished.countDown();
                    }
                }
            });
        }

        finished.await();
        assertEquals(0, executorService.shutdownNow().size());

    }

    private void testRangeSlices(RangeSliceResponseResolver resolver, Row[] expected, MessageIn<RangeSliceReply> ... messages)
    {
        for (MessageIn<RangeSliceReply> message : messages)
        {
            resolver.preprocess(message);

            List<Row> rows = resolver.getData();
            assertNotNull(rows);

            for (int i = 0; i < expected.length; i++)
                checkSame(expected[i], rows.get(i));

            Iterator<Row> rowIt = resolver.resolve().iterator();
            assertNotNull(rowIt);

            for (Row r : expected)
                checkSame(r, rowIt.next());
        }
    }

    private MessageIn<ReadResponse> makeReadResponse(String address, Row row) throws UnknownHostException
    {
        return MessageIn.create(InetAddress.getByName(address),
                                new ReadResponse(row),
                                Collections.<String, byte[]>emptyMap(),
                                MessagingService.Verb.INTERNAL_RESPONSE,
                                MessagingService.current_version);
    }

    private MessageIn<RangeSliceReply> makeRangeSlice(String address, Row ... rows) throws UnknownHostException
    {
        return MessageIn.create(InetAddress.getByName(address),
                                new RangeSliceReply(Arrays.asList(rows)),
                                Collections.<String, byte[]>emptyMap(),
                                MessagingService.Verb.INTERNAL_RESPONSE,
                                MessagingService.current_version);
    }

    private void checkSame(Row r1, Row r2)
    {
        assertEquals(r1.key, r2.key);
        assertEquals(r1.cf, r2.cf);
    }
}
