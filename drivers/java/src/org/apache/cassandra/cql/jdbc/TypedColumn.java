package org.apache.cassandra.cql.jdbc;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.thrift.Column;

import java.nio.ByteBuffer;

public class TypedColumn
{
    private final Column rawColumn;

    // we cache the frequently-accessed forms: java object for value, String for name.
    // Note that {N|V}.toString() isn't always the same as Type.getString
    // (a good example is byte buffers).
    private final Object value;
    private final String nameString;
    private final AbstractType nameType, valueType;

    public TypedColumn(Column column, AbstractType comparator, AbstractType validator)
    {
        rawColumn = column;
        this.value = column.value == null ? null : validator.compose(column.value);
        nameString = comparator.getString(column.name);
        nameType = comparator;
        valueType = validator;
    }

    public Column getRawColumn()
    {
        return rawColumn;
    }
    
    public Object getValue()
    {
        return value;
    }
    
    public String getNameString()
    {
        return nameString;
    }
    
    public String getValueString()
    {
        return valueType.getString(rawColumn.value);
    }
    
    public AbstractType getNameType()
    {
        return nameType;
    }

    public AbstractType getValueType()
    {
        return valueType;
    }
}
