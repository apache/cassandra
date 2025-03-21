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

package org.apache.cassandra.constraints;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.StringType;
import org.apache.cassandra.exceptions.InvalidRequestException;


@RunWith(Parameterized.class)
public class CreateTableWithColumnNotNullConstraintValidTest extends CqlConstraintValidationTester
{
    @Parameterized.Parameter(0)
    public String typeString;

    @Parameterized.Parameter(1)
    public Object value;

    @Parameterized.Parameters(name = "{index}: typeString={0} value={1}")
    public static Collection<Object[]> data()
    {
        return Arrays.stream(CQL3Type.Native.values())
                       .filter(t -> !t.getType().isCounter() && (t.getType().isNumber() || t.getType().unwrap() instanceof StringType))
                       .map(t -> {
                   if (t.getType().isNumber())
                       return new Object[]{ t.toString(), 123};
                   return new Object[]{ t.toString(), "'fooo'"};
               }).collect(Collectors.toList());
    }

    @Test
    public void testCreateTableWithColumnNotNullCheckValid() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 " + typeString + " CHECK NOT NULL, ck2 int, v int, PRIMARY KEY (pk));");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + value + ", 2, 3)");

        // Invalid
        assertInvalidThrowMessage("Column 'ck1' has to be specified as part of this query.", InvalidRequestException.class, "INSERT INTO %s (pk, ck2, v) VALUES (1, 2, 3)");
    }
}
