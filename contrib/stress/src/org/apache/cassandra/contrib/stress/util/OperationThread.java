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
package org.apache.cassandra.contrib.stress.util;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.cassandra.contrib.stress.Session;
import org.apache.cassandra.contrib.stress.Stress;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.FBUtilities;

public abstract class OperationThread extends Thread
{
    public final int index;

    protected final Session session;
    protected final Cassandra.Client client;

    protected final Range range;

    protected Double nextGaussian = null;

    public OperationThread(int idx)
    {
        index = idx;
        session = Stress.session;

        int keysPerThread = session.getKeysPerThread();
        range = new Range((int) (keysPerThread * (idx + session.getSkipKeys())), keysPerThread * (idx + 1));

        client = session.getClient();
    }

    /**
     * def generate_values():
     *   values = []
     *   for i in xrange(0, options.cardinality):
     *       h = md5(str(i)).hexdigest()
     *       values.append(h * int(options.column_size/len(h)) + h[:options.column_size % len(h)])
     *   return values
     *
     * @return Collection of the values
     */
    protected List<String> generateValues()
    {
        List<String> values = new ArrayList<String>();

        for (int i = 0; i < session.getCardinality(); i++)
        {
            String hash = getMD5(Integer.toString(i));
            int times = session.getColumnSize() / hash.length();
            int sumReminder = session.getColumnSize() % hash.length();

            values.add(new StringBuilder(multiplyString(hash, times)).append(hash.substring(0, sumReminder)).toString());
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
        return String.format(format, Stress.randomizer.nextInt(session.getNumKeys() - 1)).getBytes();
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

            if (0 <= token && token < session.getNumKeys())
            {
                return String.format(format, (int) token).getBytes();
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
    private double nextGaussian(int mu, float sigma)
    {
        Random random = Stress.randomizer;
        Double currentState = nextGaussian;

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
        byte[] messageDigest = md.digest(input.getBytes());
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

    protected String getExceptionMessage(Exception e)
    {
        String className = e.getClass().getSimpleName();
        String message = (e instanceof InvalidRequestException) ? ((InvalidRequestException) e).getWhy() : e.getMessage();
        return (message == null) ? "(" + className + ")" : String.format("(%s): %s", className, message);
    }

}
