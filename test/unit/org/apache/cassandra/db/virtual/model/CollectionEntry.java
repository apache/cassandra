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
 * Test collection entry represents an internal collection entry of a Cassandra collection.
 */
public class CollectionEntry
{
    private final String primaryKey;
    private final String secondaryKey;
    private final long orderedKey;
    private final String value;
    private final int intValue;
    private final long longValue;
    private final double doubleValue;
    private final short shortValue;
    private final byte byteValue;
    private final boolean booleanValue;

    public CollectionEntry(String primaryKey,
                           String secondaryKey,
                           long orderedKey,
                           String value,
                           int intValue,
                           long longValue,
                           double doubleValue,
                           short shortValue,
                           byte byteValue,
                           boolean booleanValue)
    {
        this.primaryKey = primaryKey;
        this.secondaryKey = secondaryKey;
        this.orderedKey = orderedKey;
        this.value = value;
        this.intValue = intValue;
        this.longValue = longValue;
        this.doubleValue = doubleValue;
        this.shortValue = shortValue;
        this.byteValue = byteValue;
        this.booleanValue = booleanValue;
    }

    public String getPrimaryKey()
    {
        return primaryKey;
    }

    public String getSecondaryKey()
    {
        return secondaryKey;
    }

    public long getOrderedKey()
    {
        return orderedKey;
    }

    public String getValue()
    {
        return value;
    }

    public int getIntValue()
    {
        return intValue;
    }

    public long getLongValue()
    {
        return longValue;
    }

    public double getDoubleValue()
    {
        return doubleValue;
    }

    public short getShortValue()
    {
        return shortValue;
    }

    public byte getByteValue()
    {
        return byteValue;
    }

    public boolean getBooleanValue()
    {
        return booleanValue;
    }

    @Override
    public String toString()
    {
        return "CollectionEntry{" +
               "primaryKey='" + primaryKey + '\'' +
               ", secondaryKey='" + secondaryKey + '\'' +
               ", orderedKey=" + orderedKey +
               ", value='" + value + '\'' +
               ", intValue=" + intValue +
               ", longValue=" + longValue +
               ", doubleValue=" + doubleValue +
               ", shortValue=" + shortValue +
               ", byteValue=" + byteValue +
               ", booleanValue=" + booleanValue +
               '}';
    }
}
