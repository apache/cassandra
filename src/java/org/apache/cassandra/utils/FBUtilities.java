/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils;

import java.io.*;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.util.*;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.apache.log4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;

public class FBUtilities
{
    private static Logger logger_ = Logger.getLogger(FBUtilities.class);

    private static InetAddress localInetAddress_;

    public static String[] strip(String string, String token)
    {
        StringTokenizer st = new StringTokenizer(string, token);
        List<String> result = new ArrayList<String>();
        while ( st.hasMoreTokens() )
        {
            result.add( (String)st.nextElement() );
        }
        return result.toArray( new String[0] );
    }

    public static InetAddress getLocalAddress()
    {
        if (localInetAddress_ == null)
            try
            {
                localInetAddress_ = DatabaseDescriptor.getListenAddress() == null
                                    ? InetAddress.getLocalHost()
                                    : DatabaseDescriptor.getListenAddress();
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        return localInetAddress_;
    }
    
    public static byte[] toByteArray(int i)
    {
        byte[] bytes = new byte[4];
        bytes[0] = (byte)( ( i >>> 24 ) & 0xFF);
        bytes[1] = (byte)( ( i >>> 16 ) & 0xFF);
        bytes[2] = (byte)( ( i >>> 8 ) & 0xFF);
        bytes[3] = (byte)( i & 0xFF );
        return bytes;
    }

    public static int byteArrayToInt(byte[] bytes)
    {
    	return byteArrayToInt(bytes, 0);
    }

    public static int byteArrayToInt(byte[] bytes, int offset)
    {
        if ( bytes.length - offset < 4 )
        {
            throw new IllegalArgumentException("An integer must be 4 bytes in size.");
        }
        int n = 0;
        for ( int i = 0; i < 4; ++i )
        {
            n <<= 8;
            n |= bytes[offset + i] & 0xFF;
        }
        return n;
    }

    public static int compareByteArrays(byte[] bytes1, byte[] bytes2){
        if(null == bytes1){
            if(null == bytes2) return 0;
            else return -1;
        }
        if(null == bytes2) return 1;

        for(int i = 0; i < bytes1.length && i < bytes2.length; i++){
            int cmp = compareBytes(bytes1[i], bytes2[i]);
            if(0 != cmp) return cmp;
        }
        if(bytes1.length == bytes2.length) return 0;
        else return (bytes1.length < bytes2.length)? -1 : 1;
    }

    public static int compareBytes(byte b1, byte b2){
        return compareBytes((int)b1, (int)b2);
    }

    public static int compareBytes(int b1, int b2){
        int i1 = b1 & 0xFF;
        int i2 = b2 & 0xFF;
        if(i1 < i2) return -1;
        else if(i1 == i2) return 0;
        else return 1;
    }

    public static BigInteger hash(String data)
    {
        byte[] result = hash(HashingSchemes.MD5, data.getBytes());
        BigInteger hash = new BigInteger(result);
        return hash.abs();        
    }

    public static byte[] hash(String type, byte[]... data)
    {
    	byte[] result = null;
    	try
        {
            MessageDigest messageDigest = MessageDigest.getInstance(type);
            for(byte[] block : data)
                messageDigest.update(block);
            result = messageDigest.digest();
    	}
    	catch (Exception e)
        {
    		if (logger_.isDebugEnabled())
                logger_.debug(LogUtil.throwableToString(e));
    	}
    	return result;
	}

    // The given byte array is compressed onto the specified stream.
    // The method does not close the stream. The caller will have to do it.
    public static void compressToStream(byte[] input, ByteArrayOutputStream bos) throws IOException
    {
    	 // Create the compressor with highest level of compression
        Deflater compressor = new Deflater();
        compressor.setLevel(Deflater.BEST_COMPRESSION);

        // Give the compressor the data to compress
        compressor.setInput(input);
        compressor.finish();

        // Write the compressed data to the stream
        byte[] buf = new byte[1024];
        while (!compressor.finished())
        {
            int count = compressor.deflate(buf);
            bos.write(buf, 0, count);
        }
    }


    public static byte[] decompress(byte[] compressedData, int off, int len) throws IOException, DataFormatException
    {
    	 // Create the decompressor and give it the data to compress
        Inflater decompressor = new Inflater();
        decompressor.setInput(compressedData, off, len);

        // Create an expandable byte array to hold the decompressed data
        ByteArrayOutputStream bos = new ByteArrayOutputStream(compressedData.length);

        // Decompress the data
        byte[] buf = new byte[1024];
        while (!decompressor.finished())
        {
            int count = decompressor.inflate(buf);
            bos.write(buf, 0, count);
        }
        bos.close();

        // Get the decompressed data
        return bos.toByteArray();
    }

    public static void writeByteArray(byte[] bytes, DataOutput out) throws IOException
    {
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static byte[] hexToBytes(String str)
    {
        assert str.length() % 2 == 0;
        byte[] bytes = new byte[str.length()/2];
        for (int i = 0; i < bytes.length; i++)
        {
            bytes[i] = (byte)Integer.parseInt(str.substring(i*2, i*2+2), 16);
        }
        return bytes;
    }

    public static String bytesToHex(byte[] bytes)
    {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes)
        {
            int bint = b & 0xff;
            if (bint <= 0xF)
                // toHexString does not 0 pad its results.
                sb.append("0");
            sb.append(Integer.toHexString(bint));
        }
        return sb.toString();
    }

    public static String mapToString(Map<?,?> map)
    {
        StringBuilder sb = new StringBuilder("");

        for (Map.Entry entry : map.entrySet())
        {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append(", ");
        }

        return sb.append("}").toString();
    }

    public static void writeNullableString(String key, DataOutput dos) throws IOException
    {
        dos.writeBoolean(key == null);
        if (key != null)
        {
            dos.writeUTF(key);
        }
    }

    public static String readNullableString(DataInput dis) throws IOException
    {
        if (dis.readBoolean())
            return null;
        return dis.readUTF();
    }
}
