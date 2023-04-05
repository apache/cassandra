/*
 *
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
 *
 */
package org.apache.cassandra.cql3.selection;

import java.util.List;

import com.google.common.collect.Multimap;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.cql3.ColumnSpecification;

/**
 * Represents a mapping between the actual columns used to satisfy a Selection
 * and the column definitions included in the resultset metadata for the query.
 */
public interface SelectionColumns
{
    List<ColumnSpecification> getColumnSpecifications();
    Multimap<ColumnSpecification, ColumnMetadata> getMappings();
}
