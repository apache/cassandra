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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertArrayEquals;

@RunWith(BMUnitRunner.class)
public class AccordListTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordListTest.class);

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AccordTestBase.setupClass();
        SHARED_CLUSTER.schemaChange("CREATE TYPE " + KEYSPACE + ".person (height int, age int)");
    }

    public static volatile long midTime;


    public static void trx1PreAcceptEntryWait()
    {
        midTime = System.currentTimeMillis();
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    }

    @Test
    @BMRule(name = "testListAddition",
            targetClass = "org.apache.cassandra.db.rows.AbstractCell",
            targetMethod = "updateAllTimestampAndLocalDeletionTime")
    public void testListAddition() throws Exception
    {
        SHARED_CLUSTER.schemaChange("CREATE TABLE " + currentTable + " (k int PRIMARY KEY, l list<int>)");
        SHARED_CLUSTER.forEach(node -> node.runOnInstance(() -> AccordService.instance().setCacheSize(0)));

        CountDownLatch latch = CountDownLatch.newCountDownLatch(1);

        Vector<Integer> completionOrder = new Vector<>();
        try
        {
            for (int i=0; i<100; i++)
            {

                ForkJoinTask<?> add1 = ForkJoinPool.commonPool().submit(() -> {
                    latch.awaitThrowUncheckedOnInterrupt();
                    SHARED_CLUSTER.get(1).executeInternal("BEGIN TRANSACTION " +
                            "UPDATE " + currentTable + " SET l = l + [1] WHERE k = 1; " +
                            "COMMIT TRANSACTION");
                    completionOrder.add(1);
                });

                ForkJoinTask<?> add2 = ForkJoinPool.commonPool().submit(() -> {
                    latch.awaitThrowUncheckedOnInterrupt();
                    SHARED_CLUSTER.get(1).executeInternal("BEGIN TRANSACTION " +
                            "UPDATE " + currentTable + " SET l = l + [2] WHERE k = 1; " +
                            "COMMIT TRANSACTION");
                    completionOrder.add(2);
                });
                latch.decrement();
                add1.join();
                add2.join();

                String check = "BEGIN TRANSACTION\n" +
                        "  SELECT l FROM " + currentTable + " WHERE k=1;\n" +
                        "COMMIT TRANSACTION";

                Object[][] result = SHARED_CLUSTER.get(1).executeInternal(check);
                logger.debug("insertionOrder from SELECT: {}", result);
                List<Integer> insertionOrder = (List<Integer>) result[0][0];

                assertArrayEquals(completionOrder.toArray(), insertionOrder.toArray());

                completionOrder.clear();
                SHARED_CLUSTER.get(1).executeInternal("BEGIN TRANSACTION " +
                        "UPDATE " + currentTable + " SET l = [] WHERE k = 1; " +
                        "COMMIT TRANSACTION");
            }
        }
        finally
        {
            SHARED_CLUSTER.filters().reset();
        }
    }
    @Test
    @BMRule(name = "testListAddition",
            targetClass = "org.apache.cassandra.db.rows.AbstractCell",
            targetMethod = "updateAllTimestampAndLocalDeletionTime")
    public void testListAddTwo() throws Exception
    {
        SHARED_CLUSTER.schemaChange("CREATE TABLE " + currentTable + "Two (k int PRIMARY KEY, l list<int>)");
        SHARED_CLUSTER.forEach(node -> node.runOnInstance(() -> AccordService.instance().setCacheSize(0)));

        CountDownLatch latch = CountDownLatch.newCountDownLatch(1);

        Vector<Integer> completionOrder = new Vector<>();
        try
        {
            for (int i=0; i<100; i++)
            {

                ForkJoinTask<?> add1 = ForkJoinPool.commonPool().submit(() -> {
                    latch.awaitThrowUncheckedOnInterrupt();
                    SHARED_CLUSTER.get(1).executeInternal("BEGIN TRANSACTION " +
                            "UPDATE " + currentTable + "Two SET l = l + [1,2] WHERE k = 1; " +
                            "COMMIT TRANSACTION");
                    completionOrder.add(1);
                    completionOrder.add(2);
                });

                ForkJoinTask<?> add2 = ForkJoinPool.commonPool().submit(() -> {
                    latch.awaitThrowUncheckedOnInterrupt();
                    SHARED_CLUSTER.get(1).executeInternal("BEGIN TRANSACTION " +
                            "UPDATE " + currentTable + "Two SET l = l + [3,4] WHERE k = 1; " +
                            "COMMIT TRANSACTION");
                    completionOrder.add(3);
                    completionOrder.add(4);
                });
                latch.decrement();
                add1.join();
                add2.join();

                String check = "BEGIN TRANSACTION\n" +
                        "  SELECT l FROM " + currentTable + "Two WHERE k=1;\n" +
                        "COMMIT TRANSACTION";

                Object[][] result = SHARED_CLUSTER.get(1).executeInternal(check);
                logger.debug("insertionOrder from SELECT: {}", result);
                List<Integer> insertionOrder = (List<Integer>) result[0][0];

                assertArrayEquals(completionOrder.toArray(), insertionOrder.toArray());

                completionOrder.clear();
                SHARED_CLUSTER.get(1).executeInternal("BEGIN TRANSACTION " +
                        "UPDATE " + currentTable + "Two SET l = [] WHERE k = 1; " +
                        "COMMIT TRANSACTION");
            }
        }
        finally
        {
            SHARED_CLUSTER.filters().reset();
        }
    }
    @Test
    @BMRule(name = "testListAddition",
            targetClass = "org.apache.cassandra.db.rows.AbstractCell",
            targetMethod = "updateAllTimestampAndLocalDeletionTime")
    public void testMap() throws Exception
    {
        SHARED_CLUSTER.schemaChange("CREATE TABLE " + currentTable + "Map (k int PRIMARY KEY, m map<int,int>)");
        SHARED_CLUSTER.forEach(node -> node.runOnInstance(() -> AccordService.instance().setCacheSize(0)));

        CountDownLatch latch = CountDownLatch.newCountDownLatch(1);

        Vector<Integer> completionOrder = new Vector<>();
        try
        {
            for (int i=0; i<100; i++)
            {

                ForkJoinTask<?> add1 = ForkJoinPool.commonPool().submit(() -> {
                    latch.awaitThrowUncheckedOnInterrupt();
                    SHARED_CLUSTER.get(1).executeInternal("BEGIN TRANSACTION " +
                            "UPDATE " + currentTable + "Map SET m = m + {1:99,2:88} WHERE k = 1; " +
                            "COMMIT TRANSACTION");
                    completionOrder.add(1);
                    completionOrder.add(2);
                });

                ForkJoinTask<?> add2 = ForkJoinPool.commonPool().submit(() -> {
                    latch.awaitThrowUncheckedOnInterrupt();
                    SHARED_CLUSTER.get(1).executeInternal("BEGIN TRANSACTION " +
                            "UPDATE " + currentTable + "Map SET m = m + {3:77,4:66} WHERE k = 1; " +
                            "COMMIT TRANSACTION");
                    completionOrder.add(3);
                    completionOrder.add(4);
                });
                latch.decrement();
                add1.join();
                add2.join();

                String check = "BEGIN TRANSACTION\n" +
                        "  SELECT m FROM " + currentTable + "Map WHERE k=1;\n" +
                        "COMMIT TRANSACTION";

                Object[][] result = SHARED_CLUSTER.get(1).executeInternal(check);
                logger.debug("map keys from completionOrder: {}", completionOrder);
                logger.debug("map keys from SELECT: {}", result);
                Map<Integer,Integer> insertionOrder = (LinkedHashMap<Integer,Integer>) result[0][0];

                assertArrayEquals(Arrays.stream(completionOrder.toArray()).sorted().toArray(), insertionOrder.keySet().toArray(new Integer[0]));

                completionOrder.clear();
                SHARED_CLUSTER.get(1).executeInternal("BEGIN TRANSACTION " +
                        "UPDATE " + currentTable + "Map SET m = {} WHERE k = 1; " +
                        "COMMIT TRANSACTION");            }
        }
        finally
        {
            SHARED_CLUSTER.filters().reset();
        }
    }

}