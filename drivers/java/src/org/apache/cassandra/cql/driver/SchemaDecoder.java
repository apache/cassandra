package org.apache.cassandra.cql.driver;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaDecoder 
{
    private static final Logger logger = LoggerFactory.getLogger(SchemaDecoder.class);
    private static final String MapFormatString = "%s.%s.%s";
    
    enum Specifier
    {
        Comparator,
        Validator
    }
    
    private Map<String, CfDef> cfDefs = new HashMap<String, CfDef>();
    
    // cache the comparators for efficiency.
    private Map<String, AbstractType> comparators = new HashMap<String, AbstractType>();
    
    public SchemaDecoder(List<KsDef> defs)
    {
        for (KsDef ks : defs) 
            for (CfDef cf : ks.getCf_defs())
                cfDefs.put(String.format("%s.%s", ks.getName(), cf.getName()), cf);
    }

    /**
     * @param keyspace ALWAYS specify
     * @param columnFamily ALWAYS specify
     * @param specifier ALWAYS specify
     * @param def avoids additional map lookup if specified. null is ok. though.
     * @return
     */
    private AbstractType getComparator(String keyspace, String columnFamily, Specifier specifier, CfDef def) 
    {
        // check cache first.
        String key = String.format(MapFormatString, keyspace, columnFamily, specifier.name());
        AbstractType comparator = comparators.get(key);

        // make and put in cache.
        if (comparator == null) 
        {
            if (def == null)
                def = cfDefs.get(String.format("%s.%s", keyspace, columnFamily));
            try 
            {
                switch (specifier)
                {
                    case Validator:
                        comparator = FBUtilities.getComparator(def.getDefault_validation_class());
                        break;
                    case Comparator:
                    default:
                        comparator = FBUtilities.getComparator(def.getComparator_type());
                        break;
                }
                comparators.put(key, comparator);
            }
            catch (ConfigurationException ex)
            {
                throw new RuntimeException(ex);
            }
        }
        return comparator;
    }
    
    public String colNameAsString(String keyspace, String columnFamily, String name) 
    {
        AbstractType comparator = getComparator(keyspace, columnFamily, Specifier.Comparator, null);
        ByteBuffer bb = comparator.fromString(name);
        return comparator.getString(bb);
    }
    
    public String colNameAsString(String keyspace, String columnFamily, byte[] name) 
    {
        AbstractType comparator = getComparator(keyspace, columnFamily, Specifier.Comparator, null);
        return comparator.getString(ByteBuffer.wrap(name));
    }
    
    public Col makeCol(String keyspace, String columnFamily, byte[] name, byte[] value)
    {
        CfDef cfDef = cfDefs.get(String.format("%s.%s", keyspace, columnFamily));
        AbstractType comparator = getComparator(keyspace, columnFamily, Specifier.Comparator, cfDef);
        AbstractType validator = getComparator(keyspace, columnFamily, Specifier.Validator, null);
        // todo: generate less garbage.
        return new Col(comparator.compose(ByteBuffer.wrap(name)), validator.compose(ByteBuffer.wrap(value)));
    }
}
