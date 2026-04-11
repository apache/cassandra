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
import org.apache.cassandra.cql3.constraints.InvalidConstraintDefinitionException;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;


@RunWith(Parameterized.class)
public class CreateTableWithColumnNotNullConstraintInvalidTest extends CqlConstraintValidationTester
{

    @Parameterized.Parameter
    public String typeString;


    @Parameterized.Parameters()
    public static Collection<Object[]> data()
    {
        return Arrays.stream(CQL3Type.Native.values())
                     .filter(t -> !t.getType().isCounter()
                                  && !(t.getType().unwrap() instanceof EmptyType)
                                  && !(t.getType().unwrap() instanceof DurationType))
                     .map(Object::toString)
                     .distinct()
                     .map(t -> new Object[]{ t })
                     .collect(Collectors.toList());
    }

    @Test
    public void testCreateTableWithColumnNotNullCheckNonExisting() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 " + typeString + " CHECK NOT NULL, ck2 int, v int, PRIMARY KEY (pk));");

        // Invalid
        assertInvalidThrowMessage("Column 'ck1' has to be specified as part of this query.", InvalidRequestException.class, "INSERT INTO %s (pk, ck2, v) VALUES (1, 2, 3)");

        assertInvalidThrowMessage("Column value does not satisfy value constraint for column 'ck1' as it is null.", InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, null, 2, 3)");
        assertInvalidThrowMessage("Column 'ck1' can not be set to null.", InvalidRequestException.class, "DELETE ck1 FROM %s WHERE pk = 1");
    }

    @Test
    public void testInvalidSpecificationOfNotNullConstraintOnPrimaryKeys() throws Throwable
    {
        assertThatThrownBy(() -> createTable("CREATE TABLE %s (pk " + typeString + " CHECK NOT NULL PRIMARY KEY)"))
        .isInstanceOf(InvalidRequestException.class)
        .hasRootCauseInstanceOf(InvalidConstraintDefinitionException.class)
        .hasRootCauseMessage("NOT_NULL constraint can not be specified on a partition key column 'pk'");

        assertThatThrownBy(() -> createTable("CREATE TABLE %s (pk int, cl " + typeString + " CHECK NOT NULL, PRIMARY KEY (pk, cl))"))
        .isInstanceOf(InvalidRequestException.class)
        .hasRootCauseInstanceOf(InvalidConstraintDefinitionException.class)
        .hasRootCauseMessage("NOT_NULL constraint can not be specified on a clustering key column 'cl'");
    }
}
