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

package org.apache.cassandra.db;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;

import static org.apache.cassandra.db.LegacyLayout.stringify;

/**
 * Exception thrown when we attempt to decode a legacy cellname
 * and the column name component refers to a primary key column.
 */
public class IllegalLegacyColumnException extends Exception
{
    public final ByteBuffer columnName;

    public IllegalLegacyColumnException(CFMetaData metaData, ByteBuffer columnName)
    {
        super(String.format("Illegal cell name for CQL3 table %s.%s. %s is defined as a primary key column",
                            metaData.ksName, metaData.cfName, stringify(columnName)));
        this.columnName = columnName;
    }
}
