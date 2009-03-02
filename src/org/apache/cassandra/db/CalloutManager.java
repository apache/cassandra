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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.procedures.GroovyScriptRunner;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;

public class CalloutManager
{
    private final static Logger logger_ = Logger.getLogger(CalloutManager.class); 
    private static final String extn_ = ".groovy";
    /* Used to lock the factory for creation of CalloutManager instance */
    private static Lock createLock_ = new ReentrantLock();
    /* An instance of the CalloutManager  */
    private static CalloutManager instance_;
    
    public static CalloutManager instance()
    {
        if ( instance_ == null )
        {
            CalloutManager.createLock_.lock();
            try
            {
                if ( instance_ == null )
                {
                    instance_ = new CalloutManager();
                }
            }
            finally
            {
                CalloutManager.createLock_.unlock();
            }
        }
        return instance_;
    }
    
    /* Map containing the name of callout as key and the callout script as value */
    private Map<String, CompiledScript> calloutCache_ = new HashMap<String, CompiledScript>();    
    /* The Groovy Script compiler instance */
    private Compilable compiler_;
    /* The Groovy script invokable instance */
    private Invocable invokable_;
    
    private CalloutManager()
    {
        ScriptEngineManager scriptManager = new ScriptEngineManager();
        ScriptEngine groovyEngine = scriptManager.getEngineByName("groovy");
        compiler_ = (Compilable)groovyEngine;
        invokable_ = (Invocable)groovyEngine;
    }
    
    /**
     * Compile the script and cache the compiled script.
     * @param script to be compiled
     * @throws ScriptException
     */
    private void compileAndCache(String scriptId, String script) throws ScriptException
    {
        if ( compiler_ != null )
        {
            CompiledScript compiledScript = compiler_.compile(script);
            calloutCache_.put(scriptId, compiledScript);
        }
    }
    
    /**
     * Invoked on start up to load all the stored callouts, compile
     * and cache them.
     * 
     * @throws IOException
     */
    void onStart() throws IOException
    {
    	String location = DatabaseDescriptor.getCalloutLocation();
    	if ( location == null )
    		return;
    	
        File directory = new File(location);        
        
        if ( !directory.exists() )
            directory.mkdir();
        
        File[] files = directory.listFiles();
        
        for ( File file : files )
        {
            String f = file.getName();
            /* Get the callout name from the file */
            String callout = f.split(extn_)[0];
            FileInputStream fis = new FileInputStream(file);
            byte[] bytes = new byte[fis.available()];
            fis.read(bytes);
            fis.close();
            /* cache the callout after compiling it */
            try
            {
                compileAndCache(callout, new String(bytes));                    
            }
            catch ( ScriptException ex )
            {
                logger_.warn(LogUtil.throwableToString(ex));
            }
        }
    }
    
    /**
     * Store the callout in cache and write it out
     * to disk.
     * @param callout the name of the callout
     * @param script actual implementation of the callout
    */
    public void addCallout(String callout, String script) throws IOException
    {
        /* cache the script */
        /* cache the callout after compiling it */
        try
        {
            compileAndCache(callout, script);                    
        }
        catch ( ScriptException ex )
        {
            logger_.warn(LogUtil.throwableToString(ex));
        }
        /* save the script to disk */
        String scriptFile = DatabaseDescriptor.getCalloutLocation() + System.getProperty("file.separator") + callout + extn_;
        File file = new File(scriptFile);
        if ( file.exists() )
        {
            logger_.debug("Deleting the old script file ...");
            file.delete();
        }
        FileOutputStream fos = new FileOutputStream(scriptFile);
        fos.write(script.getBytes());
        fos.close();
    }
    
    /**
     * Remove the registered callout and delete the
     * script on the disk.
     * @param callout to be removed
     */
    public void removeCallout(String callout)
    {
        /* remove the script from cache */
        calloutCache_.remove(callout);
        String scriptFile = DatabaseDescriptor.getCalloutLocation() + System.getProperty("file.separator") + callout + ".grv";
        File file = new File(scriptFile);
        file.delete();
    }
    
    /**
     * Execute the specified callout.
     * @param callout to be executed.
     * @params args arguments to be passed to the callouts.
     */
    public Object executeCallout(String callout, Object ... args)
    {
        Object result = null;
        CompiledScript script = calloutCache_.get(callout);
        if ( script != null )
        {
            try
            {
                Bindings binding = new SimpleBindings();
                binding.put("args", args);
                result = script.eval(binding);
            }
            catch(ScriptException ex)
            {
                logger_.warn(LogUtil.throwableToString(ex));
            }
        }
        return result;
    }
}
