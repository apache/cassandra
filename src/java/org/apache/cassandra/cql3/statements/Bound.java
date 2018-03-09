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

import org.apache.cassandra.schema.ColumnMetadata;

public enum Bound
{
    START(0), END(1);

    public final int idx;

    Bound(int idx)
    {
        this.idx = idx;
    }

    /**
     * Reverses the bound if the column type is a reversed one.
     *
     * @param columnMetadata the column definition
     * @return the bound reversed if the column type was a reversed one or the original bound
     */
    public Bound reverseIfNeeded(ColumnMetadata columnMetadata)
    {
        return columnMetadata.isReversedType() ? reverse() : this;
    }

    public Bound reverse()
    {
        return isStart() ? END : START;
    }

    public boolean isStart()
    {
        return this == START;
    }

    public boolean isEnd()
    {
        return this == END;
    }
}
