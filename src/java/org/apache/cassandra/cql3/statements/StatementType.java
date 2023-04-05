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

public enum StatementType
{
    INSERT
    {
        @Override
        public boolean allowClusteringColumnSlices()
        {
            return false;
        }
    },
    UPDATE
    {

        @Override
        public boolean allowClusteringColumnSlices()
        {
            return false;
        }
    },
    DELETE
    {
    },
    SELECT
    {
        @Override
        public boolean allowPartitionKeyRanges()
        {
            return true;
        }

        @Override
        public boolean allowNonPrimaryKeyInWhereClause()
        {
            return true;
        }

        @Override
        public boolean allowUseOfSecondaryIndices()
        {
            return true;
        }
    };

    /**
     * Checks if this type is an insert.
     * @return <code>true</code> if this type is an insert, <code>false</code> otherwise.
     */
    public boolean isInsert()
    {
        return this == INSERT;
    }

    /**
     * Checks if this type is an update.
     * @return <code>true</code> if this type is an update, <code>false</code> otherwise.
     */
    public boolean isUpdate()
    {
        return this == UPDATE;
    }

    /**
     * Checks if this type is a delete.
     * @return <code>true</code> if this type is a delete, <code>false</code> otherwise.
     */
    public boolean isDelete()
    {
        return this == DELETE;
    }

    /**
     * Checks if this type is a select.
     * @return <code>true</code> if this type is a select, <code>false</code> otherwise.
     */
    public boolean isSelect()
    {
        return this == SELECT;
    }

    /**
     * Checks this statement allow the where clause to contains missing partition key components or token relation.
     * @return <code>true</code> if this statement allow the where clause to contains missing partition key components
     * or token relation, <code>false</code> otherwise.
     */
    public boolean allowPartitionKeyRanges()
    {
        return false;
    }

    /**
     * Checks this type of statement allow the where clause to contains clustering column slices.
     * @return <code>true</code> if this type of statement allow the where clause to contains clustering column slices,
     * <code>false</code> otherwise.
     */
    public boolean allowClusteringColumnSlices()
    {
        return true;
    }

    /**
     * Checks if this type of statement allow non primary key in the where clause.
     * @return <code>true</code> if this type of statement allow non primary key in the where clause,
     * <code>false</code> otherwise.
     */
    public boolean allowNonPrimaryKeyInWhereClause()
    {
        return false;
    }

    /**
     * Checks if this type of statement allow the use of secondary indices.
     * @return <code>true</code> if this type of statement allow the use of secondary indices,
     * <code>false</code> otherwise.
     */
    public boolean allowUseOfSecondaryIndices()
    {
        return false;
    }
}
