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

package org.apache.cassandra.io.util;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;

import com.sun.jna.LastErrorException;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;

import org.apache.log4j.Logger;

import com.sun.jna.Native;
import org.apache.cassandra.utils.CLibrary;


public class FileUtils
{
    private static Logger logger_ = Logger.getLogger(FileUtils.class);
    private static final DecimalFormat df_ = new DecimalFormat("#.##");
    private static final double kb_ = 1024d;
    private static final double mb_ = 1024*1024d;
    private static final double gb_ = 1024*1024*1024d;
    private static final double tb_ = 1024*1024*1024*1024d;

    public static void deleteWithConfirm(File file) throws IOException
    {
        assert file.exists() : "attempted to delete non-existing file " + file.getName();
        if (logger_.isDebugEnabled())
            logger_.debug("Deleting " + file.getName());
        if (!file.delete())
        {
            throw new IOException("Failed to delete " + file.getAbsolutePath());
        }
    }

    public static class FileComparator implements Comparator<File>
    {
        public int compare(File f, File f2)
        {
            return (int)(f.lastModified() - f2.lastModified());
        }
    }

    public static void createDirectory(String directory) throws IOException
    {
        File file = new File(directory);
        if (!file.exists())
        {
            if (!file.mkdirs())
            {
                throw new IOException("unable to mkdirs " + directory);
            }
        }
    }

    public static void createFile(String directory) throws IOException
    {
        File file = new File(directory);
        if ( !file.exists() )
            file.createNewFile();
    }

    public static boolean isExists(String filename) throws IOException
    {
        File file = new File(filename);
        return file.exists();
    }

    public static boolean delete(String file)
    {
        File f = new File(file);
        return f.delete();
    }

    public static boolean delete(List<String> files) throws IOException
    {
        boolean bVal = true;
        for ( int i = 0; i < files.size(); ++i )
        {
            String file = files.get(i);
            bVal = delete(file);
            if (bVal)
            {
            	if (logger_.isDebugEnabled())
            	  logger_.debug("Deleted file " + file);
                files.remove(i);
            }
        }
        return bVal;
    }

    public static void delete(File[] files) throws IOException
    {
        for ( File file : files )
        {
            file.delete();
        }
    }

    public static String stringifyFileSize(double value)
    {
        double d = 0d;
        if ( value >= tb_ )
        {
            d = value / tb_;
            String val = df_.format(d);
            return val + " TB";
        }
        else if ( value >= gb_ )
        {
            d = value / gb_;
            String val = df_.format(d);
            return val + " GB";
        }
        else if ( value >= mb_ )
        {
            d = value / mb_;
            String val = df_.format(d);
            return val + " MB";
        }
        else if ( value >= kb_ )
        {
            d = value / kb_;
            String val = df_.format(d);
            return val + " KB";
        }
        else
        {       
            String val = df_.format(value);
            return val + " bytes";
        }        
    }
    
    /**
     * Deletes all files and subdirectories under "dir".
     * @param dir Directory to be deleted
     * @throws IOException if any part of the tree cannot be deleted
     */
    public static void deleteDir(File dir) throws IOException
    {
        if (dir.isDirectory())
        {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++)
            {
                deleteDir(new File(dir, children[i]));
            }
        }

        // The directory is now empty so now it can be smoked
        deleteWithConfirm(dir);
    }
}
