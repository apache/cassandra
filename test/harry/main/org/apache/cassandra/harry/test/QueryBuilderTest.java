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

package org.apache.cassandra.harry.test;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.execution.CompiledStatement;
import org.apache.cassandra.harry.execution.QueryBuildingVisitExecutor;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.op.Visit;

import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

public class QueryBuilderTest
{
    @Test
    public void testQueryBuilder()
    {
        withRandom(rng -> {
            SchemaSpec schemaSpec = SchemaGenerators.trivialSchema("harry", "simplified", 10).generate(rng);
            QueryBuildingVisitExecutor queryBuilder = new QueryBuildingVisitExecutor(schemaSpec, (v, q) -> String.format("__START__\n%s\n__END__;", q));
            CompiledStatement compiled = queryBuilder.compile(new Visit(1,
                                                                        new Operations.Operation[]{ new Operations.SelectPartition(1, 1L) }));
            Assert.assertTrue(compiled.cql().contains("SELECT"));
            Assert.assertTrue(compiled.cql().contains("__START__"));
            Assert.assertTrue(compiled.cql().contains("__END__"));
        });
    }
}
