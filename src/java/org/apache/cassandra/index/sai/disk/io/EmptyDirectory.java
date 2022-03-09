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
package org.apache.cassandra.index.sai.disk.io;

import java.util.Collection;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

/**
 * Always empty directory. Any operations to create, delete or open index files are unsupported.
 */
public final class EmptyDirectory extends Directory
{
    public static final Directory INSTANCE = new EmptyDirectory();

    @Override
    public String[] listAll()
    {
        return new String[0];
    }

    @Override
    public void close()
    {
        // no-op
    }

    @Override
    public void deleteFile(String name)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long fileLength(String name)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sync(Collection<String> names)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void syncMetaData()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rename(String source, String dest)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexInput openInput(String name, IOContext context)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Lock obtainLock(String name)
    {
        throw new UnsupportedOperationException();
    }
}
