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

import java.nio.ByteBuffer;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * This has a lot of building blocks for CassandraServer to call to make sure it has valid input
 * -- ensuring column names conform to the declared comparator, for instance.
 *
 * The methods here mostly try to do just one part of the validation so they can be combined
 * for different needs -- supercolumns vs regular, range slices vs named, batch vs single-column.
 * (ValidateColumnPath is the main exception in that it includes table and CF validation.)
 */
public class ThriftValidation
{
    private static final Logger logger = LoggerFactory.getLogger(ThriftValidation.class);

    static void validateKey(ByteBuffer key) throws InvalidRequestException
    {
        if (key == null || key.remaining() == 0)
        {
            throw new InvalidRequestException("Key may not be empty");
        }
        // check that key can be handled by FBUtilities.writeShortByteArray
        if (key.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            throw new InvalidRequestException("Key length of " + key.remaining() +
                                              " is longer than maximum of " + FBUtilities.MAX_UNSIGNED_SHORT);
        }
    }

    public static void validateTable(String tablename) throws KeyspaceNotDefinedException
    {
        if (!DatabaseDescriptor.getTables().contains(tablename))
        {
            throw new KeyspaceNotDefinedException("Keyspace " + tablename + " does not exist");
        }
    }

    public static ColumnFamilyType validateColumnFamily(String tablename, String cfName) throws InvalidRequestException
    {
        if (cfName.isEmpty())
        {
            throw new InvalidRequestException("non-empty columnfamily is required");
        }
        ColumnFamilyType cfType = DatabaseDescriptor.getColumnFamilyType(tablename, cfName);
        if (cfType == null)
        {
            throw new InvalidRequestException("unconfigured columnfamily " + cfName);
        }
        return cfType;
    }

    /**
     * validates the tablename and all parts of the path to the column, including the column name
     */
    static void validateColumnPath(String tablename, ColumnPath column_path) throws InvalidRequestException
    {
        validateTable(tablename);
        ColumnFamilyType cfType = validateColumnFamily(tablename, column_path.column_family);
        if (cfType == ColumnFamilyType.Standard)
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
            validateColumnNames(tablename, column_path.column_family, column_path.super_column, Arrays.asList(column_path.column));
        }
        if (column_path.super_column != null)
        {
            validateColumnNames(tablename, column_path.column_family, null, Arrays.asList(column_path.super_column));
        }
    }

    static void validateColumnParent(String tablename, ColumnParent column_parent) throws InvalidRequestException
    {
        validateTable(tablename);
        ColumnFamilyType cfType = validateColumnFamily(tablename, column_parent.column_family);
        if (cfType == ColumnFamilyType.Standard)
        {
            if (column_parent.super_column != null)
            {
                throw new InvalidRequestException("columnfamily alone is required for standard CF " + column_parent.column_family);
            }
        }
        if (column_parent.super_column != null)
        {
            validateColumnNames(tablename, column_parent.column_family, null, Arrays.asList(column_parent.super_column));
        }
    }

    // column_path_or_parent is a ColumnPath for remove, where the "column" is optional even for a standard CF
    static void validateColumnPathOrParent(String tablename, ColumnPath column_path_or_parent) throws InvalidRequestException
    {
        validateTable(tablename);
        ColumnFamilyType cfType = validateColumnFamily(tablename, column_path_or_parent.column_family);
        if (cfType == ColumnFamilyType.Standard)
        {
            if (column_path_or_parent.super_column != null)
            {
                throw new InvalidRequestException("supercolumn may not be specified for standard CF " + column_path_or_parent.column_family);
            }
        }
        if (cfType == ColumnFamilyType.Super)
        {
            if (column_path_or_parent.super_column == null && column_path_or_parent.column != null)
            {
                throw new InvalidRequestException("A column cannot be specified without specifying a super column for removal on super CF " + column_path_or_parent.column_family);
            }
        }
        if (column_path_or_parent.column != null)
        {
            validateColumnNames(tablename, column_path_or_parent.column_family, column_path_or_parent.super_column, Arrays.asList(column_path_or_parent.column));
        }
        if (column_path_or_parent.super_column != null)
        {
            validateColumnNames(tablename, column_path_or_parent.column_family, null, Arrays.asList(column_path_or_parent.super_column));
        }
    }

    /**
     * Validates the column names but not the parent path or data
     */
    private static void validateColumnNames(String keyspace, String columnFamilyName, ByteBuffer superColumnName, Iterable<ByteBuffer> column_names)
            throws InvalidRequestException
    {
        if (superColumnName != null)
        {
            if (superColumnName.remaining() > IColumn.MAX_NAME_LENGTH)
                throw new InvalidRequestException("supercolumn name length must not be greater than " + IColumn.MAX_NAME_LENGTH);
            if (superColumnName.remaining() == 0)
                throw new InvalidRequestException("supercolumn name must not be empty");
            if (DatabaseDescriptor.getColumnFamilyType(keyspace, columnFamilyName) == ColumnFamilyType.Standard)
                throw new InvalidRequestException("supercolumn specified to ColumnFamily " + columnFamilyName + " containing normal columns");
        }
        AbstractType comparator = ColumnFamily.getComparatorFor(keyspace, columnFamilyName, superColumnName);
        for (ByteBuffer name : column_names)
        {
            if (name.remaining() > IColumn.MAX_NAME_LENGTH)
                throw new InvalidRequestException("column name length must not be greater than " + IColumn.MAX_NAME_LENGTH);
            if (name.remaining() == 0)
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

    public static void validateColumnNames(String keyspace, ColumnParent column_parent, Iterable<ByteBuffer> column_names) throws InvalidRequestException
    {
        validateColumnNames(keyspace, column_parent.column_family, column_parent.super_column, column_names);
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

        Comparator<ByteBuffer> orderedComparator = range.isReversed() ? comparator.reverseComparator : comparator;
        if (range.start.remaining() > 0
            && range.finish.remaining() > 0
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
            validateTtl(cosc.column);
            ThriftValidation.validateColumnPath(keyspace, new ColumnPath(cfName).setSuper_column((ByteBuffer)null).setColumn(cosc.column.name));
            validateColumnData(keyspace, cfName, cosc.column);
        }

        if (cosc.super_column != null)
        {
            ColumnParent cp = new ColumnParent(cfName).setSuper_column(cosc.super_column.name);
            for (Column c : cosc.super_column.columns)
            {
                ThriftValidation.validateColumnPath(keyspace, new ColumnPath(cfName).setSuper_column(cosc.super_column.name).setColumn(c.name));
                validateColumnData(keyspace, cp.column_family, c);
            }
        }

        if (cosc.column == null && cosc.super_column == null)
            throw new InvalidRequestException("ColumnOrSuperColumn must have one or both of Column or SuperColumn");
    }

    private static void validateTtl(Column column) throws InvalidRequestException
    {
        if (column.isSetTtl() && column.ttl <= 0)
        {
            throw new InvalidRequestException("ttl must be positive");
        }
        // if it's not set, then it should be zero -- here we are just checking to make sure Thrift doesn't change that contract with us.
        assert column.isSetTtl() || column.ttl == 0;
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
            validateColumnOrSuperColumn(keyspace, cfName, cosc);
        }
        else if (del != null)
        {
            validateDeletion(keyspace, cfName, del);
        }
        else
        {
            throw new InvalidRequestException("Mutation must have one ColumnOrSuperColumn or one Deletion");
        }
    }

    public static void validateDeletion(String keyspace, String cfName, Deletion del) throws InvalidRequestException
    {
        validateColumnFamily(keyspace, cfName);
        if (del.predicate != null)
        {
            validateSlicePredicate(keyspace, cfName, del.super_column, del.predicate);
            if (del.predicate.slice_range != null)
                throw new InvalidRequestException("Deletion does not yet support SliceRange predicates.");
        }

        if (ColumnFamilyType.Standard == DatabaseDescriptor.getColumnFamilyType(keyspace, cfName) && del.super_column != null)
        {
            String msg = String.format("deletion of super_column is not possible on a standard ColumnFamily (KeySpace=%s ColumnFamily=%s Deletion=%s)", keyspace, cfName, del);
            throw new InvalidRequestException(msg);
        }
    }

    public static void validateSlicePredicate(String keyspace, String cfName, ByteBuffer scName, SlicePredicate predicate) throws InvalidRequestException
    {
        if (predicate.column_names == null && predicate.slice_range == null)
            throw new InvalidRequestException("A SlicePredicate must be given a list of Columns, a SliceRange, or both");

        if (predicate.slice_range != null)
            validateRange(keyspace, new ColumnParent(cfName).setSuper_column(scName), predicate.slice_range);

        if (predicate.column_names != null)
            validateColumnNames(keyspace, cfName, scName, predicate.column_names);
    }

    /**
     * Validates the data part of the column (everything in the Column object but the name)
     */
    public static void validateColumnData(String keyspace, String column_family, Column column) throws InvalidRequestException
    {
        validateTtl(column);
        try
        {
            AbstractType validator = DatabaseDescriptor.getValueValidator(keyspace, column_family, column.name);
            if (validator != null)
                validator.validate(column.value);
        }
        catch (MarshalException me)
        {
            throw new InvalidRequestException(String.format("[%s][%s][%s] = [%s] failed validation (%s)",
                                                            keyspace,
                                                            column_family,
                                                            ByteBufferUtil.bytesToHex(column.name),
                                                            ByteBufferUtil.bytesToHex(column.value),
                                                            me.getMessage()));
        }
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
            validateColumnNames(keyspace, column_parent, predicate.column_names);
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
            Token startToken = p.getToken(range.start_key);
            Token endToken = p.getToken(range.end_key);
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

    public static void validateIndexClauses(String keyspace, String columnFamily, IndexClause index_clause)
    throws InvalidRequestException
    {
        if (index_clause.expressions.isEmpty())
            throw new InvalidRequestException("index clause list may not be empty");
        Set<ByteBuffer> indexedColumns = Table.open(keyspace).getColumnFamilyStore(columnFamily).getIndexedColumns();
        AbstractType nameValidator =  ColumnFamily.getComparatorFor(keyspace, columnFamily, null);

        boolean isIndexed = false;
        for (IndexExpression expression : index_clause.expressions)
        {
            try
            {
                nameValidator.validate(expression.column_name);
            }
            catch (MarshalException me)
            {
                throw new InvalidRequestException(String.format("[%s]=[%s] failed name validation (%s)",
                                                                ByteBufferUtil.bytesToHex(expression.column_name),
                                                                ByteBufferUtil.bytesToHex(expression.value),
                                                                me.getMessage()));
            }

            AbstractType valueValidator = DatabaseDescriptor.getValueValidator(keyspace, columnFamily, expression.column_name);
            try
            {
                valueValidator.validate(expression.value);
            }
            catch (MarshalException me)
            {
                throw new InvalidRequestException(String.format("[%s]=[%s] failed value validation (%s)",
                                                                ByteBufferUtil.bytesToHex(expression.column_name),
                                                                ByteBufferUtil.bytesToHex(expression.value),
                                                                me.getMessage()));
            }

            isIndexed |= expression.op.equals(IndexOperator.EQ) && indexedColumns.contains(expression.column_name);
        }

        if (!isIndexed)
            throw new InvalidRequestException("No indexed columns present in index clause with operator EQ");
    }

    public static void validateCfDef(CfDef cf_def) throws InvalidRequestException
    {
        try
        {
            ColumnFamilyType cfType = ColumnFamilyType.create(cf_def.column_type);
            if (cfType == null)
                throw new InvalidRequestException("invalid column type " + cf_def.column_type);

            DatabaseDescriptor.getComparator(cf_def.comparator_type);
            DatabaseDescriptor.getComparator(cf_def.subcomparator_type);
            DatabaseDescriptor.getComparator(cf_def.default_validation_class);
            if (cfType != ColumnFamilyType.Super && cf_def.subcomparator_type != null)
                throw new InvalidRequestException("subcomparator_type is invalid for standard columns");

            if (cf_def.column_metadata == null)
                return;

            AbstractType comparator = cfType == ColumnFamilyType.Standard
                                    ? DatabaseDescriptor.getComparator(cf_def.comparator_type)
                                    : DatabaseDescriptor.getComparator(cf_def.subcomparator_type);

            Set<String> indexNames = new HashSet<String>();
            for (ColumnDef c : cf_def.column_metadata)
            {
                // Ensure that given idx_names and auto_generated idx_names cannot collide
                String idxName = CFMetaData.indexName(cf_def.name, ColumnDefinition.fromColumnDef(c));
                if (indexNames.contains(idxName))
                    throw new InvalidRequestException("Duplicate index names " + idxName);
                indexNames.add(idxName);

                DatabaseDescriptor.getComparator(c.validation_class);

                try
                {
                    comparator.validate(c.name);
                }
                catch (MarshalException e)
                {
                    throw new InvalidRequestException(String.format("Column name %s is not valid for comparator %s",
                                                                    ByteBufferUtil.bytesToHex(c.name), cf_def.comparator_type));
                }

                if ((c.index_name != null) && (c.index_type == null))
                    throw new ConfigurationException("index_name cannot be set without index_type");

                if (cfType == ColumnFamilyType.Super && c.index_type != null)
                    throw new InvalidRequestException("Secondary indexes are not supported on supercolumns");
            }
        }
        catch (ConfigurationException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }
    }
}
