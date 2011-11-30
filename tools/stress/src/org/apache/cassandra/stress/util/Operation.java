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
package org.apache.cassandra.stress.util;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Charsets.UTF_8;

import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.stress.Session;
import org.apache.cassandra.stress.Stress;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public abstract class Operation
{
    public final int index;

    protected final Session session;
    protected static volatile Double nextGaussian = null;

    public Operation(int idx)
    {
        index = idx;
        session = Stress.session;
    }

    public Operation(Session client, int idx)
    {
        index = idx;
        session = client;
    }

    /**
     * Run operation
     * @param client Cassandra Thrift client connection
     * @throws IOException on any I/O error.
     */
    public abstract void run(Cassandra.Client client) throws IOException;

    // Utility methods

    protected List<ByteBuffer> generateValues()
    {
        if (session.averageSizeValues)
        {
            return generateRandomizedValues();
        }

        List<ByteBuffer> values = new ArrayList<ByteBuffer>();

        for (int i = 0; i < session.getCardinality(); i++)
        {
            String hash = getMD5(Integer.toString(i));
            int times = session.getColumnSize() / hash.length();
            int sumReminder = session.getColumnSize() % hash.length();

            String value = new StringBuilder(multiplyString(hash, times)).append(hash.substring(0, sumReminder)).toString();
            values.add(ByteBuffer.wrap(value.getBytes()));
        }

        return values;
    }

    /**
     * Generate values of average size specified by -S, up to cardinality specified by -C
     * @return Collection of the values
     */
    protected List<ByteBuffer> generateRandomizedValues()
    {
        List<ByteBuffer> values = new ArrayList<ByteBuffer>();

        int limit = 2 * session.getColumnSize();

        for (int i = 0; i < session.getCardinality(); i++)
        {
            byte[] value = new byte[Stress.randomizer.nextInt(limit)];
            Stress.randomizer.nextBytes(value);
            values.add(ByteBuffer.wrap(value));
        }

        return values;
    }

    /**
     * key generator using Gauss or Random algorithm
     * @return byte[] representation of the key string
     */
    protected byte[] generateKey()
    {
        return (session.useRandomGenerator()) ? generateRandomKey() : generateGaussKey();
    }

    /**
     * Random key generator
     * @return byte[] representation of the key string
     */
    private byte[] generateRandomKey()
    {
        String format = "%0" + session.getTotalKeysLength() + "d";
        return String.format(format, Stress.randomizer.nextInt(Stress.session.getNumDifferentKeys() - 1)).getBytes(UTF_8);
    }

    /**
     * Gauss key generator
     * @return byte[] representation of the key string
     */
    private byte[] generateGaussKey()
    {
        String format = "%0" + session.getTotalKeysLength() + "d";

        for (;;)
        {
            double token = nextGaussian(session.getMean(), session.getSigma());

            if (0 <= token && token < session.getNumDifferentKeys())
            {
                return String.format(format, (int) token).getBytes(UTF_8);
            }
        }
    }

    /**
     * Gaussian distribution.
     * @param mu is the mean
     * @param sigma is the standard deviation
     *
     * @return next Gaussian distribution number
     */
    private static double nextGaussian(int mu, float sigma)
    {
        Random random = Stress.randomizer;

        Double currentState = nextGaussian;
        nextGaussian = null;

        if (currentState == null)
        {
            double x2pi  = random.nextDouble() * 2 * Math.PI;
            double g2rad = Math.sqrt(-2.0 * Math.log(1.0 - random.nextDouble()));

            currentState = Math.cos(x2pi) * g2rad;
            nextGaussian = Math.sin(x2pi) * g2rad;
        }

        return mu + currentState * sigma;
    }

    /**
     * MD5 string generation
     * @param input String
     * @return md5 representation of the string
     */
    private String getMD5(String input)
    {
        MessageDigest md = FBUtilities.threadLocalMD5Digest();
        byte[] messageDigest = md.digest(input.getBytes(UTF_8));
        StringBuilder hash = new StringBuilder(new BigInteger(1, messageDigest).toString(16));

        while (hash.length() < 32)
            hash.append("0").append(hash);

        return hash.toString();
    }

    /**
     * Equal to python/ruby - 's' * times
     * @param str String to multiple
     * @param times multiplication times
     * @return multiplied string
     */
    private String multiplyString(String str, int times)
    {
        StringBuilder result = new StringBuilder();

        for (int i = 0; i < times; i++)
            result.append(str);

        return result.toString();
    }

    protected ByteBuffer columnName(int index, boolean timeUUIDComparator)
    {
        return timeUUIDComparator
                ? TimeUUIDType.instance.decompose(UUIDGen.makeType1UUIDFromHost(Session.getLocalAddress()))
                : ByteBufferUtil.bytes(String.format("C%d", index));
    }

    protected String getExceptionMessage(Exception e)
    {
        String className = e.getClass().getSimpleName();
        String message = (e instanceof InvalidRequestException) ? ((InvalidRequestException) e).getWhy() : e.getMessage();
        return (message == null) ? "(" + className + ")" : String.format("(%s): %s", className, message);
    }

    protected void error(String message) throws IOException
    {
        if (!session.ignoreErrors())
            throw new IOException(message);
        else
            System.err.println(message);
    }
}
