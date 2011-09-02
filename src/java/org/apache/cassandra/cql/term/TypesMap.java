package org.apache.cassandra.cql.term;

import java.util.HashMap;
import java.util.Map;

public class TypesMap
{
    private final static Map<String, AbstractTerm<?>> termMap = new HashMap<String, AbstractTerm<?>>();
    
    static
    {
        termMap.put("org.apache.cassandra.db.marshal.AsciiType", AsciiTerm.instance);
        termMap.put("org.apache.cassandra.db.marshal.BooleanType", BooleanTerm.instance);
        termMap.put("org.apache.cassandra.db.marshal.BytesType", BytesTerm.instance);
        termMap.put("org.apache.cassandra.db.marshal.ColumnCounterType", CounterColumnTerm.instance);
        termMap.put("org.apache.cassandra.db.marshal.DateType", DateTerm.instance);
        termMap.put("org.apache.cassandra.db.marshal.DoubleType", DoubleTerm.instance);
        termMap.put("org.apache.cassandra.db.marshal.FloatType", FloatTerm.instance);
        termMap.put("org.apache.cassandra.db.marshal.IntegerType", IntegerTerm.instance);
        termMap.put("org.apache.cassandra.db.marshal.LexicalUUIDType", LexicalUUIDTerm.instance);
        termMap.put("org.apache.cassandra.db.marshal.LongType", LongTerm.instance);
        termMap.put("org.apache.cassandra.db.marshal.TimeUUIDType", TimeUUIDTerm.instance);
        termMap.put("org.apache.cassandra.db.marshal.UTF8Type", UTF8Term.instance);
        termMap.put("org.apache.cassandra.db.marshal.UUIDType", UUIDTerm.instance);
    }
    
    public static AbstractTerm<?> getTermForComparator(String comparator) {
        // If not fully qualified, assume it's the short name for a built-in.
        if ((comparator != null) && (!comparator.contains(".")))
            return termMap.get("org.apache.cassandra.db." + comparator);
        return termMap.get(comparator);
    }
}
