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

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Common class for the conditions that a CQL Constraint needs to implement to be integrated in the
 * CQL Constraints framework.
 */
public interface ConstraintCondition
{
    IVersionedAsymmetricSerializer<ConstraintCondition, ConstraintCondition> getSerializer();

    /**
     * Method that evaluates the condition. It can either succeed or throw a {@link ConstraintViolationException}.
     *
     * @param columnValues Column values to be evaluated at write time.
     * @param columnMetadata Metadata of the column in which the constraint is defined.
     * @param tableMetadata Metadata of the table in which the constraint is defined.
     */
    void evaluate(Map<String, String> columnValues, ColumnMetadata columnMetadata, TableMetadata tableMetadata) throws ConstraintViolationException;

    /**
     * Method to validate the condition. Method to validate the condition. This method is called when creating constraint via CQL.
     * A {@link ConstraintInvalidException} is thrown for invalid consrtaint definition.
     *
     * @param columnMetadata Metadata of the column in which the constraint is defined.
     * @param tableMetadata Metadata of the table in which the constraint is defined.
     */
    void validate(Map<String, ColumnMetadata> columnMetadata, TableMetadata tableMetadata) throws ConstraintInvalidException;
}
