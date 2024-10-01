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

package org.apache.cassandra.cql3;

import java.io.IOException;
import java.util.Map;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.UserFunctions;
import org.apache.cassandra.tcm.serialization.Version;

/**
 * Common class for the conditions that a CQL Constraint needs to implement to be integrated in the
 * CQL Constraints framework.
 */
public interface ConstraintCondition
{



    /**
     *
     * Generic condition serializer.
     *
     * @param constraintFunctionCondition
     * @param out
     * @param version
     * @throws IOException
     */
    void serialize(ConstraintCondition constraintFunctionCondition, DataOutputPlus out, Version version) throws IOException;

    /**
     * Generig condition deserializer.
     *
     * @param in
     * @param keyspace
     * @param columnType
     * @param types
     * @param functions
     * @param version
     * @return
     * @throws IOException
     */
    ConstraintCondition deserialize(DataInputPlus in, String keyspace, AbstractType<?> columnType, Types types, UserFunctions functions, Version version) throws IOException;

    /**
     * Method that provides the execution of the condition. It can either succeed or throw a {@link ConstraintViolationException}.
     *
     * @param columnValues
     * @param columnMetadata
     * @param tableMetadata
     */
    void checkCondition(Map<String, String> columnValues, ColumnMetadata columnMetadata, TableMetadata tableMetadata);

    /**
     * Method that validates that a condition is valid. This method is called when the CQL constraint is created to determine
     * if the CQL statement is valid or needs to be rejected as invalid.
     *
     * @param columnMetadata
     * @param tableMetadata
     */
    void validateCondition(ColumnMetadata columnMetadata, TableMetadata tableMetadata);
}
