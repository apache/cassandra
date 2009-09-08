package org.apache.cassandra.locator;

import java.io.IOException;

/**
 * CustomEndPointSnitchMBean
 * 
 * CustomEndPointSnitchMBean is the management interface for Digg's EndpointSnitch MBean
 * 
 * @author Sammy Yu <syu@sammyyu.net>
 * 
 */
public interface CustomEndPointSnitchMBean {
    /**
     * The object name of the mbean.
     */
    public static String MBEAN_OBJECT_NAME = "org.apache.cassandra.locator:type=EndPointSnitch";
    
    /**
     * Reload the rack configuration
     */
    public void reloadConfiguration() throws IOException;
    
    /**
     * Display the current configuration
     */
    public String displayConfiguration();

}
