package org.apache.cassandra.cql.driver;


public class Results
{
    private final SchemaDecoder decoder;
    private final String keyspace;
    private final String columnFamily;
    
    public Results(SchemaDecoder decoder, String keyspace, String columnFamily) 
    {
        this.decoder = decoder;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
    }
    
    public Col makeCol(byte[] name, byte[] value) {
        return decoder.makeCol(keyspace, columnFamily, name, value);
    }
    
}
