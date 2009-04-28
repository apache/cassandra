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

package org.apache.cassandra.net;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

import org.apache.log4j.Logger;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class SelectorManager extends Thread
{
    private static final Logger logger_ = Logger.getLogger(SelectorManager.class); 

    // the underlying selector used
    protected Selector selector_;

    // The static selector manager which is used by all applications
    private static SelectorManager manager_;
    
    // The static UDP selector manager which is used by all applications
    private static SelectorManager udpManager_;

    private SelectorManager(String name)
    {
        super(name);

        try
        {
            selector_ = Selector.open();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        setDaemon(false);
        start();
    }

    /**
     * Registers a new channel with the selector, and attaches the given
     * SelectionKeyHandler as the handler for the newly created key. Operations
     * which the hanlder is interested in will be called as available.
     * 
     * @param channel
     *            The channel to regster with the selector
     * @param handler
     *            The handler to use for the callbacks
     * @param ops
     *            The initial interest operations
     * @return The SelectionKey which uniquely identifies this channel
     * @exception IOException
     */
    public SelectionKey register(SelectableChannel channel,
            SelectionKeyHandler handler, int ops) throws IOException
    {
        if ((channel == null) || (handler == null))
        {
            throw new NullPointerException();
        }

        SelectionKey key = channel.register(selector_, ops, handler);
        selector_.wakeup();
        return key;
    }      

    /**
     * This method starts the socket manager listening for events. It is
     * designed to be started when this thread's start() method is invoked.
     */
    public void run()
    {
        while (true)
        {
            try
            {
                selector_.select(1000);
                doProcess();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    protected void doProcess() throws IOException
    {
        SelectionKey[] keys = selector_.selectedKeys().toArray(new SelectionKey[0]);

        for (int i = 0; i < keys.length; i++)
        {
            selector_.selectedKeys().remove(keys[i]);

            synchronized (keys[i])
            {
                SelectionKeyHandler skh = (SelectionKeyHandler) keys[i]
                        .attachment();

                if (skh != null)
                {
                    // accept
                    if (keys[i].isValid() && keys[i].isAcceptable())
                    {
                        skh.accept(keys[i]);
                    }

                    // connect
                    if (keys[i].isValid() && keys[i].isConnectable())
                    {
                        skh.connect(keys[i]);
                    }

                    // read
                    if (keys[i].isValid() && keys[i].isReadable())
                    {
                        skh.read(keys[i]);
                    }

                    // write
                    if (keys[i].isValid() && keys[i].isWritable())
                    {
                        skh.write(keys[i]);
                    }
                }
                else
                {
                    keys[i].channel().close();
                    keys[i].cancel();
                }
            }
        }
    }

    /**
     * Returns the SelectorManager applications should use.
     * 
     * @return The SelectorManager which applications should use
     */
    public static SelectorManager getSelectorManager()
    {
        synchronized (SelectorManager.class)
        {
            if (manager_ == null)
            {
                manager_ = new SelectorManager("TCP Selector Manager");
            }            
        }
        return manager_;
    }
    
    public static SelectorManager getUdpSelectorManager()
    {
        synchronized (SelectorManager.class)
        {
            if (udpManager_ == null)
            {
                udpManager_ = new SelectorManager("UDP Selector Manager");
            }            
        }
        return udpManager_;
    }
}
