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

package org.apache.cassandra.db.virtual.model;


/**
 * Test meter metric test representation for a {@link org.apache.cassandra.db.virtual.CollectionVirtualTableAdapter}.
 */
public class CollectionEntryTestRow
{
    private final CollectionEntry collectionEntry;

    public CollectionEntryTestRow(CollectionEntry collectionEntry)
    {
        this.collectionEntry = collectionEntry;
    }

    @Column(type = Column.Type.PARTITION_KEY)
    public String primaryKey()
    {
        return collectionEntry.getPrimaryKey();
    }

    @Column(type = Column.Type.PARTITION_KEY)
    public String secondaryKey()
    {
        return collectionEntry.getSecondaryKey();
    }

    @Column(type = Column.Type.CLUSTERING)
    public long orderedKey()
    {
        return collectionEntry.getOrderedKey();
    }

    @Column
    public long longValue()
    {
        return collectionEntry.getLongValue();
    }

    @Column
    public int intValue()
    {
        return collectionEntry.getIntValue();
    }

    @Column
    public short shortValue()
    {
        return collectionEntry.getShortValue();
    }

    @Column
    public byte byteValue()
    {
        return collectionEntry.getByteValue();
    }

    @Column
    public double doubleValue()
    {
        return collectionEntry.getDoubleValue();
    }

    @Column
    public String value()
    {
        return collectionEntry.getValue();
    }

    @Column
    public boolean booleanValue()
    {
        return collectionEntry.getBooleanValue();
    }
}
