package org.apache.cassandra.thrift;
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

import java.util.Comparator;
import java.util.Arrays;

import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MarshalException;

import static org.apache.cassandra.thrift.ThriftGlue.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class ThriftValidation
{
    static void validateKey(String key) throws InvalidRequestException
    {
        if (key.isEmpty())
        {
            throw new InvalidRequestException("Key may not be empty");
        }
        // check that writeUTF will be able to handle it -- encoded length must fit in 2 bytes
        int utflen = FBUtilities.encodedUTF8Length(key);
        if (utflen > 65535)
            throw new InvalidRequestException("Encoded key length of " + utflen + " is longer than maximum of 65535");
    }

    private static void validateTable(String tablename) throws KeyspaceNotDefinedException
    {
        if (!DatabaseDescriptor.getTables().contains(tablename))
        {
            throw new KeyspaceNotDefinedException("Keyspace " + tablename + " does not exist in this schema.");
        }
    }

    public static String validateColumnFamily(String tablename, String cfName) throws InvalidRequestException
    {
        if (cfName.isEmpty())
        {
            throw new InvalidRequestException("non-empty columnfamily is required");
        }
        String cfType = DatabaseDescriptor.getColumnType(tablename, cfName);
        if (cfType == null)
        {
            throw new InvalidRequestException("unconfigured columnfamily " + cfName);
        }
        return cfType;
    }

    static void validateColumnPath(String tablename, ColumnPath column_path) throws InvalidRequestException
    {
        validateTable(tablename);
        String cfType = validateColumnFamily(tablename, column_path.column_family);
        if (cfType.equals("Standard"))
        {
            if (column_path.super_column != null)
            {
                throw new InvalidRequestException("supercolumn parameter is invalid for standard CF " + column_path.column_family);
            }
            if (column_path.column == null)
            {
                throw new InvalidRequestException("column parameter is not optional for standard CF " + column_path.column_family);
            }
        }
        else
        {
            if (column_path.super_column == null)
                throw new InvalidRequestException("supercolumn parameter is not optional for super CF " + column_path.column_family);
        }
        if (column_path.column != null)
        {
            validateColumns(tablename, column_path.column_family, column_path.super_column, Arrays.asList(column_path.column));
        }
        if (column_path.super_column != null)
        {
            validateColumns(tablename, column_path.column_family, null, Arrays.asList(column_path.super_column));
        }
    }

    static void validateColumnParent(String tablename, ColumnParent column_parent) throws InvalidRequestException
    {
        validateTable(tablename);
        String cfType = validateColumnFamily(tablename, column_parent.column_family);
        if (cfType.equals("Standard"))
        {
            if (column_parent.super_column != null)
            {
                throw new InvalidRequestException("columnfamily alone is required for standard CF " + column_parent.column_family);
            }
        }
        if (column_parent.super_column != null)
        {
            validateColumns(tablename, column_parent.column_family, null, Arrays.asList(column_parent.super_column));
        }
    }

    // column_path_or_parent is a ColumnPath for remove, where the "column" is optional even for a standard CF
    static void validateColumnPathOrParent(String tablename, ColumnPath column_path_or_parent) throws InvalidRequestException
    {
        validateTable(tablename);
        String cfType = validateColumnFamily(tablename, column_path_or_parent.column_family);
        if (cfType.equals("Standard"))
        {
            if (column_path_or_parent.super_column != null)
            {
                throw new InvalidRequestException("supercolumn may not be specified for standard CF " + column_path_or_parent.column_family);
            }
        }
        if (column_path_or_parent.column != null)
        {
            validateColumns(tablename, column_path_or_parent.column_family, column_path_or_parent.super_column, Arrays.asList(column_path_or_parent.column));
        }
        if (column_path_or_parent.super_column != null)
        {
            validateColumns(tablename, column_path_or_parent.column_family, null, Arrays.asList(column_path_or_parent.super_column));
        }
    }

    private static void validateColumns(String keyspace, String columnFamilyName, byte[] superColumnName, Iterable<byte[]> column_names)
    throws InvalidRequestException
    {
        if (superColumnName != null)
        {
            if (superColumnName.length > IColumn.MAX_NAME_LENGTH)
                throw new InvalidRequestException("supercolumn name length must not be greater than " + IColumn.MAX_NAME_LENGTH);
            if (superColumnName.length == 0)
                throw new InvalidRequestException("supercolumn name must not be empty");
            if (!DatabaseDescriptor.getColumnFamilyType(keyspace, columnFamilyName).equals("Super"))
                throw new InvalidRequestException("supercolumn specified to ColumnFamily " + columnFamilyName + " containing normal columns");
        }
        AbstractType comparator = ColumnFamily.getComparatorFor(keyspace, columnFamilyName, superColumnName);
        for (byte[] name : column_names)
        {
            if (name.length > IColumn.MAX_NAME_LENGTH)
                throw new InvalidRequestException("column name length must not be greater than " + IColumn.MAX_NAME_LENGTH);
            if (name.length == 0)
                throw new InvalidRequestException("column name must not be empty");
            try
            {
                comparator.validate(name);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException(e.getMessage());
            }
        }
    }

    public static void validateColumns(String keyspace, ColumnParent column_parent, Iterable<byte[]> column_names) throws InvalidRequestException
    {
        validateColumns(keyspace, column_parent.column_family, column_parent.super_column, column_names);
    }

    public static void validateRange(String keyspace, ColumnParent column_parent, SliceRange range) throws InvalidRequestException
    {
        AbstractType comparator = ColumnFamily.getComparatorFor(keyspace, column_parent.column_family, column_parent.super_column);
        try
        {
            comparator.validate(range.start);
            comparator.validate(range.finish);
        }
        catch (MarshalException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }

        if (range.count < 0)
            throw new InvalidRequestException("get_slice requires non-negative count");

        Comparator<byte[]> orderedComparator = range.isReversed() ? comparator.getReverseComparator() : comparator;
        if (range.start.length > 0
            && range.finish.length > 0
            && orderedComparator.compare(range.start, range.finish) > 0)
        {
            throw new InvalidRequestException("range finish must come after start in the order of traversal");
        }
    }

    public static void validateColumnOrSuperColumn(String keyspace, String cfName, ColumnOrSuperColumn cosc)
            throws InvalidRequestException
    {
        if (cosc.column != null)
        {
            ThriftValidation.validateColumnPath(keyspace, createColumnPath(cfName, null, cosc.column.name));
        }

        if (cosc.super_column != null)
        {
            for (Column c : cosc.super_column.columns)
            {
                ThriftValidation.validateColumnPath(keyspace, createColumnPath(cfName, cosc.super_column.name, c.name));
            }
        }

        if (cosc.column == null && cosc.super_column == null)
            throw new InvalidRequestException("ColumnOrSuperColumn must have one or both of Column or SuperColumn");
    }

    public static void validateMutation(String keyspace, String cfName, Mutation mut)
            throws InvalidRequestException
    {
        ColumnOrSuperColumn cosc = mut.column_or_supercolumn;
        Deletion del = mut.deletion;

        if (cosc != null && del != null)
            throw new InvalidRequestException("Mutation may have either a ColumnOrSuperColumn or a Deletion, but not both");

        if (cosc != null)
        {
            ThriftValidation.validateColumnOrSuperColumn(keyspace, cfName, cosc);
        }
        else if (del != null)
        {
            ThriftValidation.validateDeletion(keyspace, cfName, del);
        }
        else
        {
            throw new InvalidRequestException("Mutation must have one ColumnOrSuperColumn or one Deletion");
        }
    }

    public static void validateDeletion(String keyspace, String cfName, Deletion del) throws InvalidRequestException
    {
        if (del.super_column == null && del.predicate == null)
        {
            throw new InvalidRequestException("A Deletion must have a SuperColumn, a SlicePredicate or both.");
        }

        if (del.predicate != null)
        {
            validateSlicePredicate(keyspace, cfName, del.super_column, del.predicate);
            if (del.predicate.slice_range != null)
                throw new InvalidRequestException("Deletion does not yet support SliceRange predicates.");
        }

        if (DatabaseDescriptor.getColumnFamilyType(keyspace, cfName).equals("Standard") && del.super_column != null)
        {
            String msg = String.format("deletion of super_column is not possible on a standard ColumnFamily (KeySpace=%s ColumnFamily=%s Deletion=%s)", keyspace, cfName, del);
            throw new InvalidRequestException(msg);
        }
    }

    public static void validateSlicePredicate(String keyspace, String cfName, byte[] scName, SlicePredicate predicate) throws InvalidRequestException
    {
        if (predicate.column_names == null && predicate.slice_range == null)
            throw new InvalidRequestException("A SlicePredicate must be given a list of Columns, a SliceRange, or both");

        if (predicate.slice_range != null)
            validateRange(keyspace, createColumnParent(cfName, scName), predicate.slice_range);

        if (predicate.column_names != null)
            validateColumns(keyspace, cfName, scName, predicate.column_names);
    }

    public static void validatePredicate(String keyspace, ColumnParent column_parent, SlicePredicate predicate)
            throws InvalidRequestException
    {
        if (predicate.column_names == null && predicate.slice_range == null)
            throw new InvalidRequestException("predicate column_names and slice_range may not both be null");
        if (predicate.column_names != null && predicate.slice_range != null)
            throw new InvalidRequestException("predicate column_names and slice_range may not both be present");

        if (predicate.getSlice_range() != null)
            validateRange(keyspace, column_parent, predicate.slice_range);
        else
            validateColumns(keyspace, column_parent, predicate.column_names);
    }

    public static void validateKeyRange(KeyRange range) throws InvalidRequestException
    {
        if ((range.start_key == null) != (range.end_key == null))
        {
            throw new InvalidRequestException("start key and end key must either both be non-null, or both be null");
        }
        if ((range.start_token == null) != (range.end_token == null))
        {
            throw new InvalidRequestException("start token and end token must either both be non-null, or both be null");
        }
        if ((range.start_key == null) == (range.start_token == null))
        {
            throw new InvalidRequestException("exactly one of {start key, end key} or {start token, end token} must be specified");
        }

        if (range.start_key != null)
        {
            IPartitioner p = StorageService.getPartitioner();
            Token startToken = p.decorateKey(range.start_key).token;
            Token endToken = p.decorateKey(range.end_key).token;
            if (startToken.compareTo(endToken) > 0 && !endToken.equals(p.getMinimumToken()))
            {
                if (p instanceof RandomPartitioner)
                    throw new InvalidRequestException("start key's md5 sorts after end key's md5.  this is not allowed; you probably should not specify end key at all, under RandomPartitioner");
                else
                    throw new InvalidRequestException("start key must sort before (or equal to) finish key in your partitioner!");
            }
        }

        if (range.count <= 0)
        {
            throw new InvalidRequestException("maxRows must be positive");
        }
    }
}
