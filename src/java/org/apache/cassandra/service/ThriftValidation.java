package org.apache.cassandra.service;

import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.db.ColumnFamilyNotDefinedException;
import org.apache.cassandra.config.DatabaseDescriptor;

public class ThriftValidation
{
    static void validateKeyCommand(String key, String tablename, String... columnFamilyNames) throws InvalidRequestException
    {
        validateKey(key);
        validateCommand(tablename, columnFamilyNames);
    }

    static void validateKey(String key) throws InvalidRequestException
    {
        if (key.isEmpty())
        {
            throw new InvalidRequestException("Key may not be empty");
        }
    }

    static void validateCommand(String tablename, String... columnFamilyNames) throws KeyspaceNotDefinedException, ColumnFamilyNotDefinedException
    {
        validateTable(tablename);
        for (String cfName : columnFamilyNames)
        {
            if (DatabaseDescriptor.getColumnType(tablename, cfName) == null)
            {
                throw new ColumnFamilyNotDefinedException("Column Family " + cfName + " is invalid.");
            }
        }
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
        }
        else if (column_path.super_column == null)
        {
            throw new InvalidRequestException("column parameter is not optional for super CF " + column_path.column_family);
        }
        if (column_path.column == null)
        {
            throw new InvalidRequestException("column parameter is not optional");
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
        else if (column_parent.super_column == null)
        {
            throw new InvalidRequestException("columnfamily and supercolumn are both required for super CF " + column_parent.column_family);
        }
    }

    static void validateSuperColumnPath(String tablename, SuperColumnPath super_column_path) throws InvalidRequestException
    {
        validateTable(tablename);
        String cfType = validateColumnFamily(tablename, super_column_path.column_family);
        if (cfType.equals("Standard"))
        {
            throw new InvalidRequestException(super_column_path.column_family + " is a standard columnfamily; only super columnfamilies are valid here");
        }
    }

    static void validateColumnPathOrParent(String tablename, ColumnPathOrParent column_path_or_parent) throws InvalidRequestException
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
    }
}