/*
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
package org.apache.cassandra;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import ch.qos.logback.core.OutputStreamAppender;
import ch.qos.logback.core.status.Status;
import ch.qos.logback.core.status.WarnStatus;

public class ConsoleAppender<E> extends OutputStreamAppender<E>
{
    private String target = "System.out";

    public void setTarget(String target)
    {
        if(!(target.equals("System.out") || target.equals("System.err")))
        {
            Status status = new WarnStatus("[" + target + "] should be one of System.out or System.err", this);
            status.add(new WarnStatus("Using default target System.out", this));
            addStatus(status);
            return;
        }
        this.target = target;
    }

    public String getTarget()
    {
        return target;
    }

    @Override
    public void start()
    {
        setOutputStream(new OutputStream() {
            @Override
            public void write(int b)
            {
                resolveTargetStream(target).write(b);
            }

            @Override
            public void write(byte[] b) throws IOException
            {
                resolveTargetStream(target).write(b);
            }

            @Override
            public void write(byte[] b, int off, int len)
            {
                resolveTargetStream(target).write(b, off, len);
            }

            @Override
            public void flush()
            {
                resolveTargetStream(target).flush();
            }
        });
        super.start();
    }

    private static PrintStream resolveTargetStream(String target)
    {
        if (target.equals("System.out"))
            return System.out;
        else if (target.equals("System.err"))
            return System.err;
        else
            throw new IllegalArgumentException("Target must be either 'System.out' or 'System.err'");
    }
}
