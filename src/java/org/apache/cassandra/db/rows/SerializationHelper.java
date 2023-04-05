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

import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTreeSearchIterator;

public class SerializationHelper
{
    public final SerializationHeader header;
    private BTreeSearchIterator<ColumnMetadata, ColumnMetadata> statics = null;
    private BTreeSearchIterator<ColumnMetadata, ColumnMetadata> regulars = null;

    public SerializationHelper(SerializationHeader header)
    {
        this.header = header;
    }

    private BTreeSearchIterator<ColumnMetadata, ColumnMetadata> statics()
    {
        if (statics == null)
            statics = header.columns().statics.iterator();
        return statics;
    }

    private BTreeSearchIterator<ColumnMetadata, ColumnMetadata> regulars()
    {
        if (regulars == null)
            regulars = header.columns().regulars.iterator();
        return regulars;
    }

    public SearchIterator<ColumnMetadata, ColumnMetadata> iterator(boolean isStatic)
    {
        BTreeSearchIterator<ColumnMetadata, ColumnMetadata> iterator = isStatic ? statics() : regulars();
        iterator.rewind();
        return iterator;
    }
}
