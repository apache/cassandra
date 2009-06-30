package org.apache.cassandra.service;

import org.apache.cassandra.db.TableNotDefinedException;
import org.apache.cassandra.db.ColumnFamilyNotDefinedException;
import org.apache.cassandra.db.RowMutation;
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

    static void validateCommand(String tablename, String... columnFamilyNames) throws TableNotDefinedException, ColumnFamilyNotDefinedException
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

    private static void validateTable(String tablename) throws TableNotDefinedException
    {
        if (!DatabaseDescriptor.getTables().contains(tablename))
        {
            throw new TableNotDefinedException("Table " + tablename + " does not exist in this schema.");
        }
    }

    private static String validateColumnFamily(String tablename, String[] values) throws InvalidRequestException
    {
        if (values.length < 1)
        {
            throw new InvalidRequestException("non-empty columnfamily is required");
        }
        String cfType = DatabaseDescriptor.getColumnType(tablename, values[0]);
        if (cfType == null)
        {
            throw new InvalidRequestException("unconfigured columnfamily " + values[0]);
        }
        return cfType;
    }

    static String[] validateColumnPath(String tablename, String columnPath) throws InvalidRequestException
    {
        validateTable(tablename);
        String[] values = RowMutation.getColumnAndColumnFamily(columnPath);
        String cfType = validateColumnFamily(tablename, values);
        if (cfType.equals("Standard"))
        {
            if (values.length != 2)
            {
                throw new InvalidRequestException("both parts of columnfamily:column are required for standard CF " + values[0]);
            }
        }
        else if (values.length != 3)
        {
            throw new InvalidRequestException("all parts of columnfamily:supercolumn:subcolumn are required for super CF " + values[0]);
        }
        return values;
    }

    static String[] validateColumnParent(String tablename, String columnParent) throws InvalidRequestException
    {
        validateTable(tablename);
        String[] values = RowMutation.getColumnAndColumnFamily(columnParent);
        String cfType = validateColumnFamily(tablename, values);
        if (cfType.equals("Standard"))
        {
            if (values.length != 1)
            {
                throw new InvalidRequestException("columnfamily alone is required for standard CF " + values[0]);
            }
        }
        else if (values.length != 2)
        {
            throw new InvalidRequestException("columnfamily:supercolumn is required for super CF " + values[0]);
        }
        return values;
    }

    static String[] validateSuperColumnPath(String tablename, String columnPath) throws InvalidRequestException
    {
        validateTable(tablename);
        String[] values = RowMutation.getColumnAndColumnFamily(columnPath);
        String cfType = validateColumnFamily(tablename, values);
        if (cfType.equals("Standard"))
        {
            throw new InvalidRequestException(values[0] + " is a standard columnfamily; only super columnfamilies are valid here");
        }
        else if (values.length != 1)
        {
            throw new InvalidRequestException("columnfamily alone is required for super CF " + values[0]);
        }
        return values;
    }

    static String[] validateColumnPathOrParent(String tablename, String columnPath) throws InvalidRequestException
    {
        validateTable(tablename);
        String[] values = RowMutation.getColumnAndColumnFamily(columnPath);
        String cfType = validateColumnFamily(tablename, values);
        if (cfType.equals("Standard"))
        {
            if (values.length > 2)
            {
                throw new InvalidRequestException("columnfamily:column is the most path components possible for standard CF " + values[0]);
            }
        }
        else if (values.length > 3)
        {
            throw new InvalidRequestException("columnfamily:supercolumn:column is the most path components possible for super CF " + values[0]);
        }
        return values;
    }

    public static String validateColumnFamily(String tablename, String columnFamily) throws InvalidRequestException
    {
        return validateColumnFamily(tablename, RowMutation.getColumnAndColumnFamily(columnFamily)); 
    }
}