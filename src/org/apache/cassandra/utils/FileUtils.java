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

package org.apache.cassandra.utils;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.ThreadFactoryImpl;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.log4j.Logger;



/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class FileUtils
{
    private static Logger logger_ = Logger.getLogger(FileUtils.class);
    private static final DecimalFormat df_ = new DecimalFormat("#.##");
    private static final double kb_ = 1024d;
    private static final double mb_ = 1024*1024d;
    private static final double gb_ = 1024*1024*1024d;
    private static final double tb_ = 1024*1024*1024*1024d;

    private static ExecutorService deleter_ = new DebuggableThreadPoolExecutor( 1,
            1,
            Integer.MAX_VALUE,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new ThreadFactoryImpl("FILEUTILS-DELETE-POOL")
            );

    public static void shutdown()
    {
    	deleter_.shutdownNow();
    }

    public static class Deleter implements Runnable
    {
    	File file_ = null;

    	public Deleter(File f)
        {
    		file_ = f;
        }

        public void run()
        {
        	if(file_ == null)
        		return;
        	logger_.info("*** Deleting " + file_.getName() + " ***");
        	if(!file_.delete())
        	{
            	logger_.warn("Warning : Unable to delete file " + file_.getAbsolutePath());
        	}
        }
    }

    public static class FileComparator implements Comparator<File>
    {
        public int compare(File f, File f2)
        {
            return (int)(f.lastModified() - f2.lastModified());
        }

        public boolean equals(Object o)
        {
            if ( !(o instanceof FileComparator) )
                return false;
            return true;
        }
    }

    public static void createDirectory(String directory) throws IOException
    {
        File file = new File(directory);
        if ( !file.exists() )
            file.mkdir();
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

    public static void deleteAsync(String file) throws IOException
    {
        File f = new File(file);
    	Runnable deleter = new Deleter(f);
        deleter_.submit(deleter);
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
            return val + " bytes.";
        }        
    }
    
    public static double stringToFileSize(String value)
    {        
        String[] peices = value.split(" ");
        double d = Double.valueOf(peices[0]);
        if ( peices[1].equals("TB") )
        {
            d *= tb_;
        }
        else if ( peices[1].equals("GB") )
        {
            d *= gb_;
        }
        else if ( peices[1].equals("MB") )
        {
            d *= mb_;
        }
        else if ( peices[1].equals("KB") )
        {
            d *= kb_;
        }
        else
        {
            d *= 1;
        }
        return d;
    }
    
    public static long getUsedDiskSpace()
    {
        long diskSpace = 0L;
        String[] directories = DatabaseDescriptor.getAllDataFileLocations();        
        for ( String directory : directories )
        {
            File f = new File(directory);
            File[] files = f.listFiles();
            for ( File file : files )
            {
                diskSpace += file.length();
            }
        }

        String value = df_.format(diskSpace);
        return Long.parseLong(value);
    }    
    
    
	
    /**
     * Deletes all files and subdirectories under "dir".
     * @param dir Directory to be deleted
     * @return boolean Returns "true" if all deletions were successful.
     *                 If a deletion fails, the method stops attempting to
     *                 delete and returns "false".
     */
    public static boolean deleteDir(File dir) {

        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }

        // The directory is now empty so now it can be smoked
        return dir.delete();
    }
}
