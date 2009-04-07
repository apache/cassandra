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

package org.apache.cassandra.db;

import java.io.IOException;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.IFileReader;
import org.apache.cassandra.io.SSTable;
import org.apache.cassandra.io.SequenceFile;
import org.apache.cassandra.service.StorageService;


public class FileStruct implements Comparable<FileStruct>
{
    IFileReader reader_;
    String key_; // decorated!
    DataInputBuffer bufIn_;
    DataOutputBuffer bufOut_;
    
    public FileStruct()
    {
    }
    
    public FileStruct(String file, int bufSize) throws IOException
    {
        bufIn_ = new DataInputBuffer();
        bufOut_ = new DataOutputBuffer();
        reader_ = SequenceFile.bufferedReader(file, bufSize);
        long bytesRead = advance();
        if ( bytesRead == -1L )
            throw new IOException("Either the file is empty or EOF has been reached.");          
    }
    
    public String getKey()
    {
        return key_;
    }
    
    public DataOutputBuffer getBuffer()
    {
        return bufOut_;
    }
    
    public long advance() throws IOException
    {        
        long bytesRead = -1L;
        bufOut_.reset();
        /* advance and read the next key in the file. */           
        if (reader_.isEOF())
        {
            reader_.close();
            return bytesRead;
        }
            
        bytesRead = reader_.next(bufOut_);        
        if (bytesRead == -1)
        {
            reader_.close();
            return bytesRead;
        }

        bufIn_.reset(bufOut_.getData(), bufOut_.getLength());
        key_ = bufIn_.readUTF();
        /* If the key we read is the Block Index Key then omit and read the next key. */
        if ( key_.equals(SSTable.blockIndexKey_) )
        {
            bufOut_.reset();
            bytesRead = reader_.next(bufOut_);
            if (bytesRead == -1)
            {
                reader_.close();
                return bytesRead;
            }
            bufIn_.reset(bufOut_.getData(), bufOut_.getLength());
            key_ = bufIn_.readUTF();
        }
        
        return bytesRead;
    }

    public int compareTo(FileStruct f)
    {
        return StorageService.getPartitioner().getDecoratedKeyComparator().compare(key_, f.key_);
    }
    
    public void close() throws IOException
    {
        bufIn_.close();
        bufOut_.close();
        reader_.close();
    }
}
