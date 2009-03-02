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

import java.nio.channels.*;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class SelectionKeyHandler 
{
    public void modifyKey(SelectionKey key)
    {
        throw new UnsupportedOperationException("modifyKey() cannot be called on " + getClass().getName() + "!");
    }
    
    public void modifyKeyForRead(SelectionKey key)
    {
        throw new UnsupportedOperationException("modifyKeyForRead() cannot be called on " + getClass().getName() + "!");
    }
    
    public void modifyKeyForWrite(SelectionKey key)
    {
        throw new UnsupportedOperationException("modifyKeyForWrite() cannot be called on " + getClass().getName() + "!");
    }
    
    /**
     * Method which is called when the key becomes acceptable.
     *
     * @param key The key which is acceptable.
     */
    public void accept(SelectionKey key)
    {
         throw new UnsupportedOperationException("accept() cannot be called on " + getClass().getName() + "!");
    }
    
    /**
     * Method which is called when the key becomes connectable.
     *
     * @param key The key which is connectable.
     */
    public void connect(SelectionKey key)
    {
        throw new UnsupportedOperationException("connect() cannot be called on " + getClass().getName() + "!");
    }
    
    /**
     * Method which is called when the key becomes readable.
     *
     * @param key The key which is readable.
     */
    public void read(SelectionKey key)
    {
        throw new UnsupportedOperationException("read() cannot be called on " + getClass().getName() + "!");
    }
    
    /**
     * Method which is called when the key becomes writable.
     *
     * @param key The key which is writable.
     */
    public void write(SelectionKey key)
    {
        throw new UnsupportedOperationException("write() cannot be called on " + getClass().getName() + "!");
    }
}
