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
import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;
import org.apache.cassandra.utils.*;

/**
 * This FailureDetector is an implementation of the paper titled
 * "The Phi Accrual Failure Detector" by Hayashibara. 
 * Check the paper and the <i>IFailureDetector</i> interface for details.
 * 
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */
public class FailureDetector implements IFailureDetector, FailureDetectorMBean
{
    private static Logger logger_ = Logger.getLogger(FailureDetector.class);
    private static final int sampleSize_ = 1000;
    private static final int phiSuspectThreshold_ = 5;
    private static final int phiConvictThreshold_ = 8;
    /* The Failure Detector has to have been up for atleast 1 min. */
    private static final long uptimeThreshold_ = 60000;
    private static IFailureDetector failureDetector_;
    /* Used to lock the factory for creation of FailureDetector instance */
    private static Lock createLock_ = new ReentrantLock();
    /* The time when the module was instantiated. */
    private static long creationTime_;
    
    public static IFailureDetector instance()
    {        
        if ( failureDetector_ == null )
        {
            FailureDetector.createLock_.lock();
            try
            {
                if ( failureDetector_ == null )
                {
                    failureDetector_ = new FailureDetector();
                }
            }
            finally
            {
                createLock_.unlock();
            }
        }        
        return failureDetector_;
    }
    
    private Map<EndPoint, ArrivalWindow> arrivalSamples_ = new Hashtable<EndPoint, ArrivalWindow>();
    private List<IFailureDetectionEventListener> fdEvntListeners_ = new ArrayList<IFailureDetectionEventListener>();
    
    public FailureDetector()
    {
        creationTime_ = System.currentTimeMillis();
        // Register this instance with JMX
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(this, new ObjectName("com.facebook.infrastructure.gms:type=FailureDetector"));
        }
        catch (Exception e)
        {
            logger_.error(LogUtil.throwableToString(e));
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
        catch(Throwable th)
        {
            logger_.warn(LogUtil.throwableToString(th));
        }
    }
    
    /**
     * We dump the arrival window for any endpoint only if the 
     * local Failure Detector module has been up for more than a 
     * minute.
     * 
     * @param ep for which the arrival window needs to be dumped.
     */
    private void dumpInterArrivalTimes(EndPoint ep)
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
        catch(Throwable th)
        {
            logger_.warn(LogUtil.throwableToString(th));
        }
    }
    
    public boolean isAlive(EndPoint ep)
    {
        try
        {
            /* If the endpoint in question is the local endpoint return true. */
            String localHost = FBUtilities.getLocalHostName();
            if ( localHost.equals( ep.getHost() ) )
                    return true;
        }
        catch( UnknownHostException ex )
        {
            logger_.info( LogUtil.throwableToString(ex) );
        }
    	/* Incoming port is assumed to be the Storage port. We need to change it to the control port */
    	EndPoint ep2 = new EndPoint(ep.getHost(), DatabaseDescriptor.getControlPort());        
        EndPointState epState = Gossiper.instance().getEndPointStateForEndPoint(ep2);
        return epState.isAlive();
    }
    
    public void report(EndPoint ep)
    {
        long now = System.currentTimeMillis();
        ArrivalWindow hbWnd = arrivalSamples_.get(ep);
        if ( hbWnd == null )
        {
            hbWnd = new ArrivalWindow(sampleSize_);
            arrivalSamples_.put(ep, hbWnd);
        }
        hbWnd.add(now);  
    }
    
    public void intepret(EndPoint ep)
    {
        ArrivalWindow hbWnd = arrivalSamples_.get(ep);
        if ( hbWnd == null )
        {            
            return;
        }
        long now = System.currentTimeMillis();
        /* We need this so that we do not suspect a convict. */
        boolean isConvicted = false;
        double phi = hbWnd.phi(now);
        logger_.trace("PHI for " + ep + " : " + phi);
        
        /*
        if ( phi > phiConvictThreshold_ )
        {            
            isConvicted = true;     
            for ( IFailureDetectionEventListener listener : fdEvntListeners_ )
            {
                listener.convict(ep);                
            }
        }
        */
        if ( !isConvicted && phi > phiSuspectThreshold_ )
        {     
            for ( IFailureDetectionEventListener listener : fdEvntListeners_ )
            {
                listener.suspect(ep);
            }
        }        
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
        Set<EndPoint> eps = arrivalSamples_.keySet();
        
        sb.append("-----------------------------------------------------------------------");
        for ( EndPoint ep : eps )
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
    private List<Double> arrivalIntervals_;
    private int size_;
    
    ArrivalWindow(int size)
    {
        size_ = size;
        arrivalIntervals_ = new ArrayList<Double>(size);        
    }
    
    synchronized void add(double value)
    {
        if ( arrivalIntervals_.size() == size_ )
        {                          
            arrivalIntervals_.remove(0);            
        }
        
        double interArrivalTime = 0;
        if ( tLast_ > 0L )
        {                        
            interArrivalTime = (value - tLast_);            
        }   
        tLast_ = value;            
        arrivalIntervals_.add(interArrivalTime);        
    }
    
    synchronized double sum()
    {
        double sum = 0d;
        int size = arrivalIntervals_.size();
        for( int i = 0; i < size; ++i )
        {
            sum += arrivalIntervals_.get(i);
        }
        return sum;
    }
    
    synchronized double sumOfDeviations()
    {
        double sumOfDeviations = 0d;
        double mean = mean();
        int size = arrivalIntervals_.size();
        
        for( int i = 0; i < size; ++i )
        {
            sumOfDeviations += (arrivalIntervals_.get(i) - mean)*(arrivalIntervals_.get(i) - mean);
        }
        return sumOfDeviations;
    }
    
    synchronized double mean()
    {
        return sum()/arrivalIntervals_.size();
    }
    
    synchronized double variance()
    {                
        return sumOfDeviations() / (arrivalIntervals_.size());        
    }
    
    double deviation()
    {        
        return Math.sqrt(variance());
    }
    
    void clear()
    {
        arrivalIntervals_.clear();
    }
    
    double p(double t)
    {
        // Stat stat = new Stat();
        double mean = mean();        
        double deviation = deviation();   
        /* Exponential CDF = 1 -e^-lambda*x */
        double exponent = (-1)*(t)/mean;
        return 1 - ( 1 - Math.pow(Math.E, exponent) );
        // return stat.gaussianCDF(mean, deviation, t, Double.POSITIVE_INFINITY);             
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
        StringBuilder sb = new StringBuilder();    
        List<Double> arrivalIntervals = new ArrayList<Double>(arrivalIntervals_);
        int size = arrivalIntervals.size();
        for ( int i = 0; i < size; ++i )
        {
            sb.append(arrivalIntervals.get(i));
            sb.append(" ");    
        }
        return sb.toString();
    }
}

