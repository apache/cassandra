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
package org.apache.cassandra.cql3.statements;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.ColumnIdentifier;

public class IndexTarget
{
    public final ColumnIdentifier column;
    public final boolean isCollectionKeys;
    public final boolean isFullCollection;

    private IndexTarget(ColumnIdentifier column, boolean isCollectionKeys, boolean isFullCollection)
    {
        this.column = column;
        this.isCollectionKeys = isCollectionKeys;
        this.isFullCollection = isFullCollection;
    }

    public static class Raw
    {
        private final ColumnIdentifier.Raw column;
        private final boolean isCollectionKeys;
        private final boolean isFullCollection;

        private Raw(ColumnIdentifier.Raw column, boolean isCollectionKeys, boolean isFullCollection)
        {
            this.column = column;
            this.isCollectionKeys = isCollectionKeys;
            this.isFullCollection = isFullCollection;
        }

        public static Raw valuesOf(ColumnIdentifier.Raw c)
        {
            return new Raw(c, false, false);
        }

        public static Raw keysOf(ColumnIdentifier.Raw c)
        {
            return new Raw(c, true, false);
        }

        public static Raw fullCollection(ColumnIdentifier.Raw c)
        {
            return new Raw(c, false, true);
        }

        public IndexTarget prepare(CFMetaData cfm)
        {
            return new IndexTarget(column.prepare(cfm), isCollectionKeys, isFullCollection);
        }
    }
}
