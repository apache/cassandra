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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class FBUtilities
{

    private static InetAddress localInetAddress_;
    private static String host_;

    public static String getTimestamp()
    {
        Date date = new Date();
        DateFormat df  = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        return df.format(date);
    }
    
    public static String getTimestamp(long value)
    {
        Date date = new Date(value);
        DateFormat df  = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        return df.format(date);
    }

    public static int getBits(int x, int p, int n)
    {
         return ( x >>> (p + 1 - n)) & ~(~0 << n);
    }

    public static String getCurrentThreadStackTrace()
    {
        Throwable throwable = new Throwable();
        StackTraceElement[] ste = throwable.getStackTrace();
        StringBuffer sbuf = new StringBuffer();

        for ( int i = ste.length - 1; i > 0; --i )
        {
            sbuf.append(ste[i].getClassName() + "." + ste[i].getMethodName());
            sbuf.append("/");
        }
        sbuf.deleteCharAt(sbuf.length() - 1);
        return sbuf.toString();
    }

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

    public static byte[] serializeToStream(Object o)
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] bytes = new byte[0];
        try
        {
    		ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(o);
            oos.flush();
    		bytes = bos.toByteArray();
    		oos.close();
    		bos.close();
        }
        catch ( IOException e )
        {
            LogUtil.getLogger(FBUtilities.class.getName()).info( LogUtil.throwableToString(e) );
        }
		return bytes;
    }

    public static Object deserializeFromStream(byte[] bytes)
    {
        Object o = null;
		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        try
        {
    		ObjectInputStream ois = new ObjectInputStream(bis);
            try
            {
    		    o = ois.readObject();
            }
            catch ( ClassNotFoundException e )
            {
            }
    		ois.close();
    		bis.close();
        }
        catch ( IOException e )
        {
            LogUtil.getLogger(FBUtilities.class.getName()).info( LogUtil.throwableToString(e) );
        }
		return o;
    }

    public static InetAddress getLocalAddress() throws UnknownHostException
    {
	if ( localInetAddress_ == null )
		localInetAddress_ = InetAddress.getLocalHost();
        return localInetAddress_;
    }

    public static String getHostName() throws UnknownHostException
    {
        if (DatabaseDescriptor.getListenAddress() != null)
        {
            return DatabaseDescriptor.getListenAddress();
        }
        return getLocalAddress().getCanonicalHostName();
    }

    public static boolean isHostLocalHost(InetAddress host)
    {
        try {
            return getLocalAddress().equals(host);
        }
        catch ( UnknownHostException e )
        {
            return false;
        }
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

    public static boolean isEqualBits(byte[] bytes1, byte[] bytes2)
    {
        return 0 == compareByteArrays(bytes1, bytes2);
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

    public static String stackTrace(Throwable e)
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter( sw );
        e.printStackTrace(pw);
        return sw.toString();
    }

    public static BigInteger hash(String data)
    {
        byte[] result = hash(HashingSchemes.MD5, data.getBytes());
        BigInteger hash = new BigInteger(result);
        return hash.abs();        
    }

    public static byte[] hash(String type, byte[] data)
    {
    	byte[] result = null;
    	try
        {
    		MessageDigest messageDigest = MessageDigest.getInstance(type);
    		result = messageDigest.digest(data);
    	}
    	catch (Exception e)
        {
    		LogUtil.getLogger(FBUtilities.class.getName()).debug(LogUtil.throwableToString(e));
    	}
    	return result;
	}

    public static boolean isEqual(byte[] digestA, byte[] digestB)
    {
        return MessageDigest.isEqual(digestA, digestB);
    }

    // The given bytearray is compressed onto the specifed stream.
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


    public static byte[] compress(byte[] input) throws IOException
    {
        // Create an expandable byte array to hold the compressed data.
        // You cannot use an array that's the same size as the orginal because
        // there is no guarantee that the compressed data will be smaller than
        // the uncompressed data.
        ByteArrayOutputStream bos = new ByteArrayOutputStream(input.length);
        compressToStream(input,bos);
        bos.close();

        // Get the compressed data
        return bos.toByteArray();
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

    public static byte[] decompress(byte[] compressedData) throws IOException, DataFormatException
    {
    	return decompress(compressedData, 0, compressedData.length);
    }

     public static byte[] xor(byte[] b1, byte[] b2)
     {
    	 byte[] bLess = null;
    	 byte[] bMore = null;

    	 if(b1.length > b2.length)
    	 {
    		 bLess = b2;
    		 bMore = b1;
    	 }
    	 else
    	 {
    		 bLess = b1;
    		 bMore = b2;
    	 }

    	 for(int i = 0 ; i< bLess.length; i++ )
    	 {
    		 bMore[i] = (byte)(bMore[i] ^ bLess[i]);
    	 }

    	 return bMore;
     }

     public static int getUTF8Length(String string)
     {
     	/*
     	 * We store the string as UTF-8 encoded, so when we calculate the length, it
     	 * should be converted to UTF-8.
     	 */
     	String utfName  = string;
     	int length = utfName.length();
     	try
     	{
     		//utfName  = new String(string.getBytes("UTF-8"));
     		length = string.getBytes("UTF-8").length;
     	}
     	catch (UnsupportedEncodingException e)
     	{
     		LogUtil.getLogger(FBUtilities.class.getName()).info(LogUtil.throwableToString(e));
     	}

     	return length;
     }
}
