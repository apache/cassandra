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

package org.apache.cassandra.cql3.functions.masking;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.BeforeClass;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.SchemaConstants;

import static java.lang.String.format;

/**
 * Tests for {@link HashMaskingFunction}.
 */
public class HashMaskingFunctionTest extends MaskingFunctionTester
{
    @BeforeClass
    public static void beforeClass()
    {
        requireNetwork();
    }

    @Override
    protected void testMaskingOnColumn(String name, CQL3Type type, Object value) throws Throwable
    {
        ByteBuffer serializedValue = serializedValue(type, value);

        // with default algorithm
        assertRows(execute(format("SELECT mask_hash(%s) FROM %%s", name)),
                   row(HashMaskingFunction.hash(HashMaskingFunction.messageDigest(HashMaskingFunction.DEFAULT_ALGORITHM),
                                                serializedValue)));

        // with null algorithm
        assertRows(execute(format("SELECT mask_hash(%s, null) FROM %%s", name)),
                   row(HashMaskingFunction.hash(HashMaskingFunction.messageDigest(HashMaskingFunction.DEFAULT_ALGORITHM),
                                                serializedValue)));

        // with manually specified algorithm
        assertRows(execute(format("SELECT mask_hash(%s, 'SHA-512') FROM %%s", name)),
                   row(HashMaskingFunction.hash(HashMaskingFunction.messageDigest("SHA-512"), serializedValue)));

        // with not found ASCII algorithm
        assertInvalidThrowMessage("Hash algorithm not found",
                                  InvalidRequestException.class,
                                  format("SELECT mask_hash(%s, 'unknown-algorithm') FROM %%s", name));

        // with not found UTF-8 algorithm
        assertInvalidThrowMessage("Hash algorithm not found",
                                  InvalidRequestException.class,
                                  format("SELECT mask_hash(%s, 'áéíóú') FROM %%s", name));

        // test result set metadata, it should always be of type blob
        ResultSet rs = executeNet(format("SELECT mask_hash(%s) FROM %%s", name));
        ColumnDefinitions definitions = rs.getColumnDefinitions();
        Assert.assertEquals(1, definitions.size());
        Assert.assertEquals(DataType.blob(), definitions.getType(0));
        Assert.assertEquals(format("%s.mask_hash(%s)", SchemaConstants.SYSTEM_KEYSPACE_NAME, name),
                            definitions.getName(0));
    }

    private ByteBuffer serializedValue(CQL3Type type, Object value)
    {
        if (isNullOrEmptyMultiCell(type, value))
            return null;

        return makeByteBuffer(value, type.getType());
    }
}
