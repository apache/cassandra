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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.TreeSet;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;
import org.apache.cassandra.utils.*;
/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class SelectorManager extends Thread
{
    private static final Logger logger_ = Logger.getLogger(SelectorManager.class); 
    // the underlying selector used
    /**
     * DESCRIBE THE FIELD
     */
    protected Selector selector_;   
    protected HashSet<SelectionKey> modifyKeysForRead_;    
    protected HashSet<SelectionKey> modifyKeysForWrite_;
    
    // the list of keys waiting to be cancelled
    protected HashSet<SelectionKey> cancelledKeys_;    

    // The static selector manager which is used by all applications
    private static SelectorManager manager_;
    
    // The static UDP selector manager which is used by all applications
    private static SelectorManager udpManager_;

    /**
     * Constructor, which is private since there is only one selector per JVM.
     * 
     * @param name name of thread. 
     */
    protected SelectorManager(String name)
    {
        super(name);                        
        this.modifyKeysForRead_ = new HashSet<SelectionKey>();
        this.modifyKeysForWrite_ = new HashSet<SelectionKey>();
        this.cancelledKeys_ = new HashSet<SelectionKey>();

        // attempt to create selector
        try
        {
            selector_ = Selector.open();
        }
        catch (IOException e)
        {
            logger_.error("SEVERE ERROR (SelectorManager): Error creating selector "
                            + e);
        }

        setDaemon(false);
        start();
    }

    /**
     * Method which asks the Selector Manager to add the given key to the
     * cancelled set. If noone calls register on this key during the rest of
     * this select() operation, the key will be cancelled. Otherwise, it will be
     * returned as a result of the register operation.
     * 
     * @param key
     *            The key to cancel
     */
    public void cancel(SelectionKey key)
    {
        if (key == null)
        {
            throw new NullPointerException();
        }

        synchronized ( cancelledKeys_ )
        {
            cancelledKeys_.add(key);
        }
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
     *                DESCRIBE THE EXCEPTION
     */
    public SelectionKey register(SelectableChannel channel,
            SelectionKeyHandler handler, int ops) throws IOException
    {
        if ((channel == null) || (handler == null))
        {
            throw new NullPointerException();
        }

        selector_.wakeup();
        SelectionKey key = channel.register(selector_, ops, handler);
        synchronized(cancelledKeys_)
        {
            cancelledKeys_.remove(key);
        }
        selector_.wakeup();
        return key;
    }      
    
    public void modifyKeyForRead(SelectionKey key)
    {
        if (key == null)
        {
            throw new NullPointerException();
        }

        synchronized(modifyKeysForRead_)
        {
            modifyKeysForRead_.add(key);
        }
        selector_.wakeup();
    }
    
    public void modifyKeyForWrite(SelectionKey key)
    {
        if (key == null)
        {
            throw new NullPointerException();
        }

        synchronized( modifyKeysForWrite_ )
        {
            modifyKeysForWrite_.add(key);
        }
        selector_.wakeup();
    }

    /**
     * This method starts the socket manager listening for events. It is
     * designed to be started when this thread's start() method is invoked.
     */
    public void run()
    {
        try
        {              
            // loop while waiting for activity
            while (true && !Thread.currentThread().interrupted() )
            { 
                try
                {
                    doProcess();                                                                          
                    selector_.select(1000); 
                    synchronized( cancelledKeys_ )
                    {
                        if (cancelledKeys_.size() > 0)
                        {
                            SelectionKey[] keys = cancelledKeys_.toArray( new SelectionKey[0]);                        
                            
                            for ( SelectionKey key : keys )
                            {
                                key.cancel();
                                key.channel().close();
                            }                                                
                            cancelledKeys_.clear();
                        }
                    }
                }
                catch ( IOException e )
                {
                    logger_.warn(LogUtil.throwableToString(e));
                }
            }
                         
            manager_ = null;
        }
        catch (Throwable t)
        {
            logger_.error("ERROR (SelectorManager.run): " + t);
            logger_.error(LogUtil.throwableToString(t));
            System.exit(-1);
        }
    }    

    protected void doProcess() throws IOException
    {
        doInvocationsForRead();
        doInvocationsForWrite();
        doSelections();
    }
    
    /**
     * DESCRIBE THE METHOD
     * 
     * @exception IOException
     *                DESCRIBE THE EXCEPTION
     */
    protected void doSelections() throws IOException
    {
        SelectionKey[] keys = selectedKeys();

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
    
    private void doInvocationsForRead()
    {
        Iterator<SelectionKey> it;
        synchronized (modifyKeysForRead_)
        {
            it = new ArrayList<SelectionKey>(modifyKeysForRead_).iterator();
            modifyKeysForRead_.clear();
        }

        while (it.hasNext())
        {
            SelectionKey key = it.next();
            if (key.isValid() && (key.attachment() != null))
            {
                ((SelectionKeyHandler) key.attachment()).modifyKeyForRead(key);
            }
        }
    }
    
    private void doInvocationsForWrite()
    {
        Iterator<SelectionKey> it;
        synchronized (modifyKeysForWrite_)
        {
            it = new ArrayList<SelectionKey>(modifyKeysForWrite_).iterator();
            modifyKeysForWrite_.clear();
        }

        while (it.hasNext())
        {
            SelectionKey key = it.next();
            if (key.isValid() && (key.attachment() != null))
            {
                ((SelectionKeyHandler) key.attachment()).modifyKeyForWrite(key);
            }
        }
    }

    /**
     * Selects all of the currenlty selected keys on the selector and returns
     * the result as an array of keys.
     * 
     * @return The array of keys
     * @exception IOException
     *                DESCRIBE THE EXCEPTION
     */
    protected SelectionKey[] selectedKeys() throws IOException
    {
        return (SelectionKey[]) selector_.selectedKeys().toArray(
                new SelectionKey[0]);
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
    
    /**
     * Returns whether or not this thread of execution is the selector thread
     * 
     * @return Whether or not this is the selector thread
     */
    public static boolean isSelectorThread()
    {
        return (Thread.currentThread() == manager_);
    }
}
