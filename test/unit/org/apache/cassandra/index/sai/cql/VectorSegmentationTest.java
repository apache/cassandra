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

package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;

public class VectorSegmentationTest extends SAITester
{
    @Test
    public void test() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, val vector<float, 4>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        setSegmentWriteBufferSpace(5000);

        for (int row = 0; row < 10000; row++)
            execute("INSERT INTO %s (pk, val) VALUES (?, ?)", row, nextVector());

        UntypedResultSet resultSet = execute("SELECT * FROM %s WHERE val ANN OF ? LIMIT 10", nextVector());

        System.out.println(makeRowStrings(resultSet));
    }

    private Vector nextVector()
    {
        Float[] vector = new Float[4];

        for (int index = 0; index < vector.length; index++)
            vector[index] = getRandom().nextFloat();

        return new Vector<Float>(vector);
    }
}
