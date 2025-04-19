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

package org.apache.cassandra.db.rows;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;

public abstract class AbstractRowIterator extends AbstractIterator<Row> implements RowIterator
{
    protected final TableMetadata metadata;
    protected final DecoratedKey key;
    protected final RegularAndStaticColumns columns;
    protected final boolean isReversedOrder;
    protected final Row staticRow;

    public AbstractRowIterator(TableMetadata metadata, DecoratedKey key, RegularAndStaticColumns columns, boolean isReversedOrder, Row staticRow)
    {
        this.metadata = metadata;
        this.key = key;
        this.columns = columns;
        this.isReversedOrder = isReversedOrder;
        this.staticRow = staticRow;
    }

    @Override
    public TableMetadata metadata()
    {
        return metadata;
    }

    @Override
    public DecoratedKey partitionKey()
    {
        return key;
    }

    @Override
    public RegularAndStaticColumns columns()
    {
        return columns;
    }

    @Override
    public Row staticRow()
    {
        return staticRow;
    }

    @Override
    public boolean isReverseOrder()
    {
        return isReversedOrder;
    }
}
