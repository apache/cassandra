/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.cql.jdbc;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.cassandra.serializers.DecimalSerializer;
import org.junit.Assert;
import org.junit.Test;

public class JdbcDecimalTest
{
    @Test
    public void testComposeDecompose()
    {
        BigDecimal expected = new BigDecimal("123456789123456789.987654321");
        DecimalSerializer decimal = new DecimalSerializer();
        
        ByteBuffer buffer = decimal.serialize(expected);
        BigDecimal actual = decimal.deserialize(buffer);
        Assert.assertEquals(expected, actual);
    }
}
