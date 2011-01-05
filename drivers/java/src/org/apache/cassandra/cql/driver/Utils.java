package org.apache.cassandra.cql.driver;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;

import org.apache.cassandra.thrift.Compression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils
{
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);
    
    public static ByteBuffer compressQuery(String queryStr, Compression compression)
    {
        byte[] data = queryStr.getBytes();
        Deflater compressor = new Deflater();
        compressor.setInput(data);
        compressor.finish();
        
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        
        while (!compressor.finished())
        {
            int size = compressor.deflate(buffer);
            byteArray.write(buffer, 0, size);
        }
        
        logger.trace("Compressed query statement {} bytes in length to {} bytes",
                     data.length,
                     byteArray.size());
        
        return ByteBuffer.wrap(byteArray.toByteArray());
    }
}
