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
package org.apache.cassandra.index.sasi.memory;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.index.sasi.utils.TypeUtil;
import org.apache.cassandra.utils.FBUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexMemtable
{
    private static final Logger logger = LoggerFactory.getLogger(IndexMemtable.class);

    private final MemIndex index;

    public IndexMemtable(ColumnIndex columnIndex)
    {
        this.index = MemIndex.forColumn(columnIndex.keyValidator(), columnIndex);
    }

    public long index(DecoratedKey key, ByteBuffer value)
    {
        if (value == null || value.remaining() == 0)
            return 0;

        AbstractType<?> validator = index.columnIndex.getValidator();
        if (!TypeUtil.isValid(value, validator))
        {
            int size = value.remaining();
            if ((value = TypeUtil.tryUpcast(value, validator)) == null)
            {
                logger.error("Can't add column {} to index for key: {}, value size {}, validator: {}.",
                             index.columnIndex.getColumnName(),
                             index.columnIndex.keyValidator().getString(key.getKey()),
                             FBUtilities.prettyPrintMemory(size),
                             validator);
                return 0;
            }
        }

        return index.add(key, value);
    }

    public RangeIterator<Long, Token> search(Expression expression)
    {
        return index == null ? null : index.search(expression);
    }
}
