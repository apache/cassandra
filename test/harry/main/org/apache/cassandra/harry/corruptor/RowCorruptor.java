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

package org.apache.cassandra.harry.corruptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.harry.data.ResultSetRow;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.operations.CompiledStatement;

public interface RowCorruptor
{
    final Logger logger = LoggerFactory.getLogger(QueryResponseCorruptor.class);

    boolean canCorrupt(ResultSetRow row);

    CompiledStatement corrupt(ResultSetRow row);

    // Returns true if it could corrupt a row, false otherwise
    default boolean maybeCorrupt(ResultSetRow row, SystemUnderTest sut)
    {
        if (canCorrupt(row))
        {
            CompiledStatement statement = corrupt(row);
            sut.execute(statement.cql(), SystemUnderTest.ConsistencyLevel.ALL, statement.bindings());
            logger.info("Corrupting with: {} ({})", statement.cql(), CompiledStatement.bindingsToString(statement.bindings()));
            return true;
        }
        return false;
    }

    interface RowCorruptorFactory
    {
        RowCorruptor create(SchemaSpec schemaSpec,
                            OpSelectors.Clock clock);
    }
}