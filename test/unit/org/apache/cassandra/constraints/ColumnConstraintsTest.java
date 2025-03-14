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

import org.junit.Test;

import org.apache.cassandra.cql3.constraints.ColumnConstraint.ConstraintType;

import static org.apache.cassandra.cql3.constraints.ColumnConstraint.ConstraintType.COMPOSED;
import static org.apache.cassandra.cql3.constraints.ColumnConstraint.ConstraintType.FUNCTION;
import static org.apache.cassandra.cql3.constraints.ColumnConstraint.ConstraintType.SCALAR;
import static org.apache.cassandra.cql3.constraints.ColumnConstraint.ConstraintType.UNARY_FUNCTION;
import static org.junit.Assert.assertEquals;

public class ColumnConstraintsTest
{
    private static final ConstraintType[] EXPECTED_VALUES = { COMPOSED, FUNCTION, SCALAR, UNARY_FUNCTION };

    @Test
    public void testEnumCodesAndNames()
    {
        ConstraintType[] values = ConstraintType.values();

        for (int i = 0; i < values.length; i++)
        {
            assertEquals("Column Constraint Serializer mismatch in the enum " + values[i],
                         EXPECTED_VALUES[i].name(), values[i].name());
            assertEquals("Column Constraint Serializer mismatch in the enum for value " + values[i],
                         ConstraintType.getSerializer(EXPECTED_VALUES[i].ordinal()), ConstraintType.getSerializer(i));
        }

        assertEquals("Column Constraint Serializer enum constants has changed. Update the test.",
                     EXPECTED_VALUES.length, values.length);
    }
}
