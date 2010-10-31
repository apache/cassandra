package org.apache.cassandra.cql;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;

import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.specific.SpecificRequestor;
import org.apache.cassandra.avro.Cassandra;
import org.apache.cassandra.avro.Column;
import org.apache.cassandra.avro.Compression;
import org.apache.cassandra.avro.CqlResult;
import org.apache.cassandra.avro.CqlRow;

public class Client
{
    public static void main(String[] args) throws IOException
    {
        // Remote setup
        String host = "localhost", keyspace = "Keyspace1";
        int port = 9160;
        
        HttpTransceiver tr = new HttpTransceiver(new URL("http", host, port, ""));
        Cassandra client = (Cassandra)SpecificRequestor.getClient(Cassandra.class, tr);
        client.set_keyspace(keyspace);
        
        // Query compression
        String query = "SELECT FROM Standard2 USING CONSISTENCY.ONE WHERE KEY = \"eevans\" AND COL < \"age\" COLLIMIT 2 ASC;";
        Deflater compressor = new Deflater();
        compressor.setInput(query.getBytes());
        compressor.finish();
        byte[] output = new byte[100];
        System.out.println("Query compressed from " + query.length() + " bytes, to " + compressor.deflate(output) + " bytes");
        
        CqlResult res = client.execute_cql_query(ByteBuffer.wrap(output), Compression.GZIP);
        
        switch (res.type)
        {
            case ROWS:
                for (CqlRow row : res.rows)
                {
                    System.out.println("Key = " + new String(row.key.array()));
                    for (Column col : row.columns)
                        System.out.println("  Col = " + new String(col.name.array()) + "/" + new String(col.value.array()));
                }
        }
        
    }
}
