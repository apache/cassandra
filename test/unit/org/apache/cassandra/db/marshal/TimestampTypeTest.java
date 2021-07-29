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
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.Date;

import org.junit.Test;

import org.apache.cassandra.utils.AbstractTypeGenerators.TypeSupport;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.utils.AbstractTypeGenerators.getTypeSupport;
import static org.quicktheories.QuickTheory.qt;

public class TimestampTypeTest
{
    @Test
    public void stringProperty()
    {
        TypeSupport<Date> support = getTypeSupport(TimestampType.instance);
        qt().forAll(support.valueGen).checkAssert(date -> {
            ByteBuffer buffer = TimestampType.instance.decompose(date);
            String toString = TimestampType.instance.getString(buffer);
            Assertions.assertThat(TimestampType.instance.fromString(toString))
                      .as("TimestampType.fromString(TimestampType.getString(buffer)) == buffer;\nviolated with toString %s", toString)
                      .isEqualTo(buffer);
        });
    }
}
