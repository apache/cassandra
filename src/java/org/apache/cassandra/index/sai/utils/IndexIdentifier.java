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

package org.apache.cassandra.index.sai.utils;

import com.google.common.base.Objects;

/**
 * This is a simple wrapper around the index identity. Its primary purpose is to isolate classes that only need
 * access to the identity from the main index classes. This is useful in testing but also makes it easier to pass
 * the log message wrapper {@link #logMessage(String)} to classes that don't need any other information about the index.
 */
public class IndexIdentifier
{
    public final String keyspaceName;
    public final String tableName;
    public final String indexName;

    public IndexIdentifier(String keyspaceName, String tableName, String indexName)
    {
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
        this.indexName = indexName;
    }

    /**
     * A helper method for constructing consistent log messages for specific column indexes.
     * <p>
     * Example: For the index "idx" in keyspace "ks" on table "tb", calling this method with the raw message
     * "Flushing new index segment..." will produce...
     * <p>
     * "[ks.tb.idx] Flushing new index segment..."
     *
     * @param message The raw content of a logging message, without information identifying it with an index.
     *
     * @return A log message with the proper keyspace, table and index name prepended to it.
     */
    public String logMessage(String message)
    {
        // Index names are unique only within a keyspace.
        return String.format("[%s.%s.%s] %s", keyspaceName, tableName, indexName, message);
    }

    @Override
    public String toString()
    {
        return String.format("%s.%s", keyspaceName, indexName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(keyspaceName, tableName, indexName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        IndexIdentifier other = (IndexIdentifier) obj;
        return Objects.equal(keyspaceName, other.keyspaceName) &&
               Objects.equal(tableName, other.tableName) &&
               Objects.equal(indexName, other.indexName);
    }
}
