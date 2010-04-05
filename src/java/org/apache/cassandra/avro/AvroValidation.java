package org.apache.cassandra.avro;
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


import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.avro.util.Utf8;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MarshalException;

import static org.apache.cassandra.avro.ErrorFactory.newInvalidRequestException;
import static org.apache.cassandra.avro.AvroRecordFactory.newColumnPath;

/**
 * The Avro analogue to org.apache.cassandra.service.ThriftValidation
 */
public class AvroValidation {
    // FIXME: could use method in ThriftValidation
    static void validateKey(String key) throws InvalidRequestException
    {
        if (key.isEmpty())
            throw newInvalidRequestException("Key may not be empty");
    }
    
    // FIXME: could use method in ThriftValidation
    static void validateKeyspace(String keyspace) throws KeyspaceNotDefinedException
    {
        if (!DatabaseDescriptor.getTables().contains(keyspace))
            throw new KeyspaceNotDefinedException(new Utf8("Keyspace " + keyspace + " does not exist in this schema."));
    }
    
    // FIXME: could use method in ThriftValidation
    static String validateColumnFamily(String keyspace, String columnFamily) throws InvalidRequestException
    {
        if (columnFamily.isEmpty())
            throw newInvalidRequestException("non-empty columnfamily is required");
        
        String cfType = DatabaseDescriptor.getColumnFamilyType(keyspace, columnFamily);
        if (cfType == null)
            throw newInvalidRequestException("unconfigured columnfamily " + columnFamily);
        
        return cfType;
    }
    
    static void validateColumnPath(String keyspace, ColumnPath cp) throws InvalidRequestException
    {
        validateKeyspace(keyspace);
        String column_family = cp.column_family.toString();
        String cfType = validateColumnFamily(keyspace, column_family);
        
        byte[] column = null, super_column = null;
        if (cp.super_column != null) super_column = cp.super_column.array();
        if (cp.column != null) column = cp.column.array();
        
        if (cfType.equals("Standard"))
        {
            if (super_column != null)
                throw newInvalidRequestException("supercolumn parameter is invalid for standard CF " + column_family);
            
            if (column == null)
                throw newInvalidRequestException("column parameter is not optional for standard CF " + column_family);
        }
        else
        {
            if (super_column == null)
                throw newInvalidRequestException("supercolumn parameter is not optional for super CF " + column_family);
        }
         
        if (column != null)
            validateColumns(keyspace, column_family, super_column, Arrays.asList(cp.column));
        if (super_column != null)
            validateColumns(keyspace, column_family, null, Arrays.asList(cp.super_column));
    }
    
    // FIXME: could use method in ThriftValidation
    static void validateColumns(String keyspace, String cfName, byte[] superColumnName, Iterable<ByteBuffer> columnNames)
    throws InvalidRequestException
    {
        if (superColumnName != null)
        {
            if (superColumnName.length > IColumn.MAX_NAME_LENGTH)
                throw newInvalidRequestException("supercolumn name length must not be greater than " + IColumn.MAX_NAME_LENGTH);
            if (superColumnName.length == 0)
                throw newInvalidRequestException("supercolumn name must not be empty");
            if (!DatabaseDescriptor.getColumnFamilyType(keyspace, cfName).equals("Super"))
                throw newInvalidRequestException("supercolumn specified to ColumnFamily " + cfName + " containing normal columns");
        }
        
        AbstractType comparator = ColumnFamily.getComparatorFor(keyspace, cfName, superColumnName);
        for (ByteBuffer buff : columnNames)
        {
            byte[] name = buff.array();

            if (name.length > IColumn.MAX_NAME_LENGTH)
                throw newInvalidRequestException("column name length must not be greater than " + IColumn.MAX_NAME_LENGTH);
            if (name.length == 0)
                throw newInvalidRequestException("column name must not be empty");
            
            try
            {
                comparator.validate(name);
            }
            catch (MarshalException e)
            {
                throw newInvalidRequestException(e.getMessage());
            }
        }
    }

    static void validateColumnOrSuperColumn(String keyspace, String cfName, ColumnOrSuperColumn cosc)
    throws InvalidRequestException
    {
        if (cosc.column != null)
            AvroValidation.validateColumnPath(keyspace, newColumnPath(cfName, null, cosc.column.name));

        if (cosc.super_column != null)
            for (Column c : cosc.super_column.columns)
                AvroValidation.validateColumnPath(keyspace, newColumnPath(cfName, cosc.super_column.name, c.name));

        if ((cosc.column == null) && (cosc.super_column == null))
            throw newInvalidRequestException("ColumnOrSuperColumn must have one or both of Column or SuperColumn");
    }

    static void validateRange(String keyspace, String cfName, byte[] superName, SliceRange range)
    throws InvalidRequestException
    {
        AbstractType comparator = ColumnFamily.getComparatorFor(keyspace, cfName, superName);
        byte[] start = range.start.array();
        byte[] finish = range.finish.array();

        try
        {
            comparator.validate(start);
            comparator.validate(finish);
        }
        catch (MarshalException me)
        {
            throw newInvalidRequestException(me.getMessage());
        }

        if (range.count < 0)
            throw newInvalidRequestException("Ranges require a non-negative count.");

        Comparator<byte[]> orderedComparator = range.reversed ? comparator.getReverseComparator() : comparator;
        if (start.length > 0 && finish.length > 0 && orderedComparator.compare(start, finish) > 0)
            throw newInvalidRequestException("range finish must come after start in the order of traversal");
    }

    static void validateSlicePredicate(String keyspace, String cfName, byte[] superName, SlicePredicate predicate)
    throws InvalidRequestException
    {
        if (predicate.column_names == null && predicate.slice_range == null)
            throw newInvalidRequestException("A SlicePredicate must be given a list of Columns, a SliceRange, or both");

        if (predicate.slice_range != null)
            validateRange(keyspace, cfName, superName, predicate.slice_range);

        if (predicate.column_names != null)
            validateColumns(keyspace, cfName, superName, predicate.column_names);
    }

    static void validateDeletion(String keyspace, String  cfName, Deletion del) throws InvalidRequestException
    {
        if (del.super_column == null && del.predicate == null)
            throw newInvalidRequestException("A Deletion must have a SuperColumn, a SlicePredicate, or both.");

        if (del.predicate != null)
        {
            validateSlicePredicate(keyspace, cfName, del.super_column.array(), del.predicate);
            if (del.predicate.slice_range != null)
                throw newInvalidRequestException("Deletion does not yet support SliceRange predicates.");
        }
    }

    static void validateMutation(String keyspace, String cfName, Mutation mutation) throws InvalidRequestException
    {
        ColumnOrSuperColumn cosc = mutation.column_or_supercolumn;
        Deletion del = mutation.deletion;

        if (cosc != null && del != null)
            throw newInvalidRequestException("Mutation may have either a ColumnOrSuperColumn or a Deletion, but not both");

        if (cosc != null)
        {
            validateColumnOrSuperColumn(keyspace, cfName, cosc);
        }
        else if (del != null)
        {
            validateDeletion(keyspace, cfName, del);
        }
        else
        {
            throw newInvalidRequestException("Mutation must have one ColumnOrSuperColumn, or one Deletion");
        }
    }
}
