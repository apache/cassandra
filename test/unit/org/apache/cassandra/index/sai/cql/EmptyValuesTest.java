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

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.utils.AbstractTypeGenerators;

import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.quicktheories.QuickTheory.qt;

/**
 * Tests that empty values are only indexed for literal indexes. See CASSANDRA-20313 for more details.
 */
public class EmptyValuesTest extends SAITester
{
    @Test
    public void testEmptyValues()
    {
        qt().forAll(AbstractTypeGenerators.primitiveTypeGen()).checkAssert(type -> {
            CQL3Type cql3Type = type.asCQL3Type();
            if (type.allowsEmpty() && StorageAttachedIndex.SUPPORTED_TYPES.contains(cql3Type))
                testEmptyValues(cql3Type);
        });
    }

    private void testEmptyValues(CQL3Type type)
    {
        createTable(String.format("CREATE TABLE %%s (k int PRIMARY KEY, v %s)", type));
        execute("INSERT INTO %s (k, v) VALUES (0, ?)", EMPTY_BYTE_BUFFER);
        flush();
        createIndex(String.format(CREATE_INDEX_TEMPLATE, 'v'));

        IndexTermType termType = createIndexTermType(type.getType());
        boolean indexed = !termType.skipsEmptyValue();

        Assertions.assertThat(execute("SELECT * FROM %s WHERE v = ?", EMPTY_BYTE_BUFFER)).hasSize(indexed ? 1 : 0);

        execute("INSERT INTO %s (k, v) VALUES (1, ?)", EMPTY_BYTE_BUFFER);

        Assertions.assertThat(execute("SELECT * FROM %s WHERE v = ?", EMPTY_BYTE_BUFFER)).hasSize(indexed ? 2 : 0);
        flush();
        Assertions.assertThat(execute("SELECT * FROM %s WHERE v = ?", EMPTY_BYTE_BUFFER)).hasSize(indexed ? 2 : 0);
    }
}