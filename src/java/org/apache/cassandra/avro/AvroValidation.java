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
import java.util.Set;

import org.apache.avro.util.Utf8;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.avro.ErrorFactory.newInvalidRequestException;
import static org.apache.cassandra.avro.AvroRecordFactory.newColumnPath;

/**
 * The Avro analogue to org.apache.cassandra.service.ThriftValidation
 */
public class AvroValidation
{    
    public static void validateKey(ByteBuffer key) throws InvalidRequestException
    {
        if (key == null || key.remaining() == 0)
            throw newInvalidRequestException("Key may not be empty");
        
        // check that key can be handled by FBUtilities.writeShortByteArray
        if (key.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
            throw newInvalidRequestException("Key length of " + key.remaining() +
                    " is longer than maximum of " + FBUtilities.MAX_UNSIGNED_SHORT);
    }
    
    
    // FIXME: could use method in ThriftValidation
    static void validateKeyspace(String keyspace) throws KeyspaceNotDefinedException
    {
        if (!DatabaseDescriptor.getTables().contains(keyspace))
            throw new KeyspaceNotDefinedException(new Utf8("Keyspace " + keyspace + " does not exist in this schema."));
    }
    
    // FIXME: could use method in ThriftValidation
    static ColumnFamilyType validateColumnFamily(String keyspace, String columnFamily) throws InvalidRequestException
    {
        if (columnFamily.isEmpty())
            throw newInvalidRequestException("non-empty columnfamily is required");
        
        ColumnFamilyType cfType = DatabaseDescriptor.getColumnFamilyType(keyspace, columnFamily);
        if (cfType == null)
            throw newInvalidRequestException("unconfigured columnfamily " + columnFamily);
        
        return cfType;
    }
    
    static void validateColumnPath(String keyspace, ColumnPath cp) throws InvalidRequestException
    {
        validateKeyspace(keyspace);
        String column_family = cp.column_family.toString();
        ColumnFamilyType cfType = validateColumnFamily(keyspace, column_family);
        
       
        if (cfType == ColumnFamilyType.Standard)
        {
            if (cp.super_column != null)
                throw newInvalidRequestException("supercolumn parameter is invalid for standard CF " + column_family);
            
            if (cp.column == null)
                throw newInvalidRequestException("column parameter is not optional for standard CF " + column_family);
        }
        else
        {
            if (cp.super_column == null)
                throw newInvalidRequestException("supercolumn parameter is not optional for super CF " + column_family);
        }
         
        if (cp.column != null)
            validateColumns(keyspace, column_family, cp.super_column, Arrays.asList(cp.column));
        if (cp.super_column != null)
            validateColumns(keyspace, column_family, null, Arrays.asList(cp.super_column));
    }
    
    static void validateColumnParent(String keyspace, ColumnParent parent) throws InvalidRequestException
    {
        validateKeyspace(keyspace);
        String cfName = parent.column_family.toString();
        ColumnFamilyType cfType = validateColumnFamily(keyspace, cfName);
        
        if (cfType == ColumnFamilyType.Standard)
            if (parent.super_column != null)
                throw newInvalidRequestException("super column specified for standard column family");
        if (parent.super_column != null)
            validateColumns(keyspace, cfName, null, Arrays.asList(parent.super_column));
    }
    
    // FIXME: could use method in ThriftValidation
    static void validateColumns(String keyspace, String cfName, ByteBuffer superColumnName, Iterable<ByteBuffer> columnNames)
    throws InvalidRequestException
    {
        if (superColumnName != null)
        {
            if (superColumnName.remaining() > IColumn.MAX_NAME_LENGTH)
                throw newInvalidRequestException("supercolumn name length must not be greater than " + IColumn.MAX_NAME_LENGTH);
            if (superColumnName.remaining() == 0)
                throw newInvalidRequestException("supercolumn name must not be empty");
            if (DatabaseDescriptor.getColumnFamilyType(keyspace, cfName) == ColumnFamilyType.Standard)
                throw newInvalidRequestException("supercolumn specified to ColumnFamily " + cfName + " containing normal columns");
        }
        
        AbstractType comparator = ColumnFamily.getComparatorFor(keyspace, cfName, superColumnName);
        for (ByteBuffer buff : columnNames)
        {
           
            if (buff.remaining() > IColumn.MAX_NAME_LENGTH)
                throw newInvalidRequestException("column name length must not be greater than " + IColumn.MAX_NAME_LENGTH);
            if (buff.remaining() == 0)
                throw newInvalidRequestException("column name must not be empty");
            
            try
            {
                comparator.validate(buff);
            }
            catch (MarshalException e)
            {
                throw newInvalidRequestException(e.getMessage());
            }
        }
    }
    
    static void validateColumns(String keyspace, ColumnParent parent, Iterable<ByteBuffer> columnNames)
    throws InvalidRequestException
    {
        validateColumns(keyspace,
                        parent.column_family.toString(),
                        parent.super_column,
                        columnNames);
    }
    
    static void validateColumn(String keyspace, ColumnParent parent, Column column)
    throws InvalidRequestException
    {
        validateTtl(column);
        validateColumns(keyspace, parent, Arrays.asList(column.name));
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

    static void validateRange(String keyspace, String cfName, ByteBuffer superName, SliceRange range)
    throws InvalidRequestException
    {
        AbstractType comparator = ColumnFamily.getComparatorFor(keyspace, cfName, superName);
        

        try
        {
            comparator.validate(range.start);
            comparator.validate(range.finish);
        }
        catch (MarshalException me)
        {
            throw newInvalidRequestException(me.getMessage());
        }

        if (range.count < 0)
            throw newInvalidRequestException("Ranges require a non-negative count.");

        Comparator<ByteBuffer> orderedComparator = range.reversed ? comparator.getReverseComparator() : comparator;
        if (range.start.remaining() > 0 && range.finish.remaining() > 0 && orderedComparator.compare(range.start, range.finish) > 0)
            throw newInvalidRequestException("range finish must come after start in the order of traversal");
    }
    
    static void validateRange(String keyspace, ColumnParent cp, SliceRange range) throws InvalidRequestException
    {
        validateRange(keyspace, cp.column_family.toString(), cp.super_column, range);
    }

    static void validateSlicePredicate(String keyspace, String cfName, ByteBuffer superName, SlicePredicate predicate)
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
        validateColumnFamily(keyspace, cfName);
        if (del.super_column == null && del.predicate == null)
            throw newInvalidRequestException("A Deletion must have a SuperColumn, a SlicePredicate, or both.");

        if (del.predicate != null)
        {
            validateSlicePredicate(keyspace, cfName, del.super_column, del.predicate);
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
    
    static void validateTtl(Column column) throws InvalidRequestException
    {
        if (column.ttl != null && column.ttl < 0)
            throw newInvalidRequestException("ttl must be a positive value");
    }

    static void validatePredicate(String keyspace, ColumnParent cp, SlicePredicate predicate)
    throws InvalidRequestException
    {
        if (predicate.column_names == null && predicate.slice_range == null)
            throw newInvalidRequestException("predicate column_names and slice_range may not both be null");
        
        if (predicate.column_names != null && predicate.slice_range != null)
            throw newInvalidRequestException("predicate column_names and slice_range may not both be set");
        
        if (predicate.slice_range != null)
            validateRange(keyspace, cp, predicate.slice_range);
        else
            validateColumns(keyspace, cp, predicate.column_names);
    }

    public static void validateKeyRange(KeyRange range)
    throws InvalidRequestException
    {
        if ((range.start_key == null) != (range.end_key == null))
        {
            throw newInvalidRequestException("start key and end key must either both be non-null, or both be null");
        }
        if ((range.start_token == null) != (range.end_token == null))
        {
            throw newInvalidRequestException("start token and end token must either both be non-null, or both be null");
        }
        if ((range.start_key == null) == (range.start_token == null))
        {
            throw newInvalidRequestException("exactly one of {start key, end key} or {start token, end token} must be specified");
        }

        if (range.start_key != null)
        {
            IPartitioner p = StorageService.getPartitioner();
            Token startToken = p.getToken(range.start_key);
            Token endToken = p.getToken(range.end_key);
            if (startToken.compareTo(endToken) > 0 && !endToken.equals(p.getMinimumToken()))
            {
                if (p instanceof RandomPartitioner)
                    throw newInvalidRequestException("start key's md5 sorts after end key's md5.  this is not allowed; you probably should not specify end key at all, under RandomPartitioner");
                else
                    throw newInvalidRequestException("start key must sort before (or equal to) finish key in your partitioner!");
            }
        }

        if (range.count <= 0)
        {
            throw newInvalidRequestException("maxRows must be positive");
        }
    }

    static void validateIndexClauses(String keyspace, String columnFamily, IndexClause index_clause)
    throws InvalidRequestException
    {
        if (index_clause.expressions.isEmpty())
            throw newInvalidRequestException("index clause list may not be empty");
        Set<ByteBuffer> indexedColumns = Table.open(keyspace).getColumnFamilyStore(columnFamily).getIndexedColumns();
        for (IndexExpression expression : index_clause.expressions)
        {
            if (expression.op.equals(IndexOperator.EQ) && indexedColumns.contains(expression.column_name))
                return;
        }
        throw newInvalidRequestException("No indexed columns present in index clause with operator EQ");
    }

}
