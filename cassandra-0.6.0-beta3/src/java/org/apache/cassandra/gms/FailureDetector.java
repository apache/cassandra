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

package org.apache.cassandra.gms;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.IOError;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.lang.StringUtils;

import java.net.InetAddress;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.BoundedStatsDeque;
import org.apache.log4j.Logger;

/**
 * This FailureDetector is an implementation of the paper titled
 * "The Phi Accrual Failure Detector" by Hayashibara. 
 * Check the paper and the <i>IFailureDetector</i> interface for details.
 */
public class FailureDetector implements IFailureDetector, FailureDetectorMBean
{
    public static final IFailureDetector instance = new FailureDetector();
    private static Logger logger_ = Logger.getLogger(FailureDetector.class);
    private static final int sampleSize_ = 1000;
    private static final int phiConvictThreshold_ = 8;
    /* The Failure Detector has to have been up for at least 1 min. */
    private static final long uptimeThreshold_ = 60000;
    /* The time when the module was instantiated. */
    private static long creationTime_;
    
    private Map<InetAddress, ArrivalWindow> arrivalSamples_ = new Hashtable<InetAddress, ArrivalWindow>();
    private List<IFailureDetectionEventListener> fdEvntListeners_ = new ArrayList<IFailureDetectionEventListener>();
    
    public FailureDetector()
    {
        creationTime_ = System.currentTimeMillis();
        // Register this instance with JMX
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(this, new ObjectName("org.apache.cassandra.gms:type=FailureDetector"));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Dump the inter arrival times for examination if necessary.
     */
    public void dumpInterArrivalTimes()
    {
        try
        {
            FileOutputStream fos = new FileOutputStream("/var/tmp/output-" + System.currentTimeMillis() + ".dat", true);
            fos.write(toString().getBytes());
            fos.close();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }
    
    /**
     * We dump the arrival window for any endpoint only if the 
     * local Failure Detector module has been up for more than a 
     * minute.
     * 
     * @param ep for which the arrival window needs to be dumped.
     */
    private void dumpInterArrivalTimes(InetAddress ep)
    {
        long now = System.currentTimeMillis();
        if ( (now - FailureDetector.creationTime_) <= FailureDetector.uptimeThreshold_ )
            return;
        try
        {
            FileOutputStream fos = new FileOutputStream("/var/tmp/output-" + System.currentTimeMillis() + "-" + ep + ".dat", true);
            ArrivalWindow hWnd = arrivalSamples_.get(ep);
            fos.write(hWnd.toString().getBytes());
            fos.close();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }
    
    public boolean isAlive(InetAddress ep)
    {
       /* If the endpoint in question is the local endpoint return true. */
        InetAddress localHost = FBUtilities.getLocalAddress();
        if (localHost.equals(ep))
            return true;

    	/* Incoming port is assumed to be the Storage port. We need to change it to the control port */
        EndPointState epState = Gossiper.instance.getEndPointStateForEndPoint(ep);
        return epState.isAlive();
    }
    
    public void report(InetAddress ep)
    {
        if (logger_.isTraceEnabled())
            logger_.trace("reporting " + ep);
        long now = System.currentTimeMillis();
        ArrivalWindow heartbeatWindow = arrivalSamples_.get(ep);
        if ( heartbeatWindow == null )
        {
            heartbeatWindow = new ArrivalWindow(sampleSize_);
            arrivalSamples_.put(ep, heartbeatWindow);
        }
        heartbeatWindow.add(now);
    }
    
    public void interpret(InetAddress ep)
    {
        ArrivalWindow hbWnd = arrivalSamples_.get(ep);
        if ( hbWnd == null )
        {            
            return;
        }
        long now = System.currentTimeMillis();
        double phi = hbWnd.phi(now);
        if (logger_.isTraceEnabled())
            logger_.trace("PHI for " + ep + " : " + phi);
        
        if ( phi > phiConvictThreshold_ )
        {     
            for ( IFailureDetectionEventListener listener : fdEvntListeners_ )
            {
                listener.convict(ep);
            }
        }        
    }

    public void remove(InetAddress ep)
    {
        arrivalSamples_.remove(ep);
    }
    
    public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener)
    {
        fdEvntListeners_.add(listener);
    }
    
    public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener)
    {
        fdEvntListeners_.remove(listener);
    }
    
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        Set<InetAddress> eps = arrivalSamples_.keySet();
        
        sb.append("-----------------------------------------------------------------------");
        for ( InetAddress ep : eps )
        {
            ArrivalWindow hWnd = arrivalSamples_.get(ep);
            sb.append(ep + " : ");
            sb.append(hWnd.toString());
            sb.append( System.getProperty("line.separator") );
        }
        sb.append("-----------------------------------------------------------------------");
        return sb.toString();
    }
    
    public static void main(String[] args) throws Throwable
    {           
    }
}

class ArrivalWindow
{
    private static Logger logger_ = Logger.getLogger(ArrivalWindow.class);
    private double tLast_ = 0L;
    private BoundedStatsDeque arrivalIntervals_;

    ArrivalWindow(int size)
    {
        arrivalIntervals_ = new BoundedStatsDeque(size);
    }
    
    synchronized void add(double value)
    {
        double interArrivalTime;
        if ( tLast_ > 0L )
        {                        
            interArrivalTime = (value - tLast_);            
        }
        else
        {
            interArrivalTime = Gossiper.intervalInMillis_ / 2;
        }
        tLast_ = value;            
        arrivalIntervals_.add(interArrivalTime);        
    }
    
    synchronized double sum()
    {
        return arrivalIntervals_.sum();
    }
    
    synchronized double sumOfDeviations()
    {
        return arrivalIntervals_.sumOfDeviations();
    }
    
    synchronized double mean()
    {
        return arrivalIntervals_.mean();
    }
    
    synchronized double variance()
    {
        return arrivalIntervals_.variance();
    }
    
    double stdev()
    {
        return arrivalIntervals_.stdev();
    }
    
    void clear()
    {
        arrivalIntervals_.clear();
    }
    
    double p(double t)
    {
        double mean = mean();
        double exponent = (-1)*(t)/mean;
        return 1 - ( 1 - Math.pow(Math.E, exponent) );
    }
    
    double phi(long tnow)
    {            
        int size = arrivalIntervals_.size();
        double log = 0d;
        if ( size > 0 )
        {
            double t = tnow - tLast_;                
            double probability = p(t);       
            log = (-1) * Math.log10( probability );                                 
        }
        return log;           
    } 
    
    public String toString()
    {
        return StringUtils.join(arrivalIntervals_.iterator(), " ");
    }
}

