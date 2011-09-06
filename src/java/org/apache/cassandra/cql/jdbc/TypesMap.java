package org.apache.cassandra.cql.jdbc;

import java.util.HashMap;
import java.util.Map;

public class TypesMap
{
    private final static Map<String, AbstractJdbcType<?>> map = new HashMap<String, AbstractJdbcType<?>>();
    
    static
    {
        map.put("org.apache.cassandra.db.marshal.AsciiType", AsciiTerm.instance);
        map.put("org.apache.cassandra.db.marshal.BooleanType", JdbcBoolean.instance);
        map.put("org.apache.cassandra.db.marshal.BytesType", JdbcBytes.instance);
        map.put("org.apache.cassandra.db.marshal.ColumnCounterType", JdbcCounterColumn.instance);
        map.put("org.apache.cassandra.db.marshal.DateType", JdbcDate.instance);
        map.put("org.apache.cassandra.db.marshal.DoubleType", JdbcDouble.instance);
        map.put("org.apache.cassandra.db.marshal.FloatType", JdbcFloat.instance);
        map.put("org.apache.cassandra.db.marshal.IntegerType", JdbcInteger.instance);
        map.put("org.apache.cassandra.db.marshal.LexicalUUIDType", JdbcLexicalUUID.instance);
        map.put("org.apache.cassandra.db.marshal.LongType", LongTerm.instance);
        map.put("org.apache.cassandra.db.marshal.TimeUUIDType", JdbcTimeUUID.instance);
        map.put("org.apache.cassandra.db.marshal.UTF8Type", JdbcUTF8.instance);
        map.put("org.apache.cassandra.db.marshal.UUIDType", JdbcUUID.instance);
    }
    
    public static AbstractJdbcType<?> getTermForComparator(String comparator)
    {
        // If not fully qualified, assume it's the short name for a built-in.
        if ((comparator != null) && (!comparator.contains(".")))
            return map.get("org.apache.cassandra.db." + comparator);
        return map.get(comparator);
    }
}
