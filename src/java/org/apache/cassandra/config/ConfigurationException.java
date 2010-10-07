package org.apache.cassandra.config;

public class ConfigurationException extends Exception
{
    public ConfigurationException(String message)
    {
        super(message);
    }
}
