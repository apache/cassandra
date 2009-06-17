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

package org.apache.cassandra.service;

import java.util.Comparator;

import org.apache.cassandra.utils.FileUtils;


class LoadInfo
{
    protected static class DiskSpaceComparator implements Comparator<LoadInfo>
    {
        public int compare(LoadInfo li, LoadInfo li2)
        {
            if ( li == null || li2 == null )
                throw new IllegalArgumentException("Cannot pass in values that are NULL.");
            
            double space = FileUtils.stringToFileSize(li.diskSpace_);
            double space2 = FileUtils.stringToFileSize(li2.diskSpace_);
            return (int)(space - space2);
        }
    }
        
    private String diskSpace_;
    
    LoadInfo(long diskSpace)
    {       
        diskSpace_ = FileUtils.stringifyFileSize(diskSpace);
    }
    
    LoadInfo(String loadInfo)
    {
        diskSpace_ = loadInfo;
    }
    
    String diskSpace()
    {
        return diskSpace_;
    }
    
    public String toString()
    {
        StringBuilder sb = new StringBuilder("");       
        sb.append(diskSpace_);
        return sb.toString();
    }
}
