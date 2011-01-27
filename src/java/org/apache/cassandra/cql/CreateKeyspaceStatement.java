package org.apache.cassandra.cql;

import java.util.HashMap;
import java.util.Map;
import org.apache.cassandra.thrift.InvalidRequestException;

/** A <code>CREATE KEYSPACE</code> statement parsed from a CQL query. */
public class CreateKeyspaceStatement
{   
    private final String name;
    private final Map<String, String> attrs;
    private String strategyClass;
    private int replicationFactor;
    private Map<String, String> strategyOptions = new HashMap<String, String>();
    
    /**
     * Creates a new <code>CreateKeyspaceStatement</code> instance for a given
     * keyspace name and keyword arguments.
     *  
     * @param name the name of the keyspace to create
     * @param attrs map of the raw keyword arguments that followed the <code>WITH</code> keyword.
     */
    public CreateKeyspaceStatement(String name, Map<String, String> attrs)
    {
        this.name = name;
        this.attrs = attrs;
    }
    
    /**
     * The <code>CqlParser</code> only goes as far as extracting the keyword arguments
     * from these statements, so this method is responsible for processing and
     * validating, and must be called prior to access.
     * 
     * @throws InvalidRequestException if arguments are missing or unacceptable
     */
    public void validate() throws InvalidRequestException
    {   
        // required
        if (!attrs.containsKey("strategy_class"))
            throw new InvalidRequestException("missing required argument \"strategy_class\"");
        strategyClass = attrs.get("strategy_class");
        
        // required
        if (!attrs.containsKey("replication_factor"))
            throw new InvalidRequestException("missing required argument \"replication_factor\"");
        
        try
        {
            replicationFactor = Integer.parseInt(attrs.get("replication_factor"));
        }
        catch (NumberFormatException e)
        {
            throw new InvalidRequestException(String.format("\"%s\" is not valid for replication_factor",
                                                            attrs.get("replication_factor")));
        }
        
        // optional
        for (String key : attrs.keySet())
            if ((key.contains(":")) && (key.startsWith("strategy_options")))
                strategyOptions.put(key.split(":")[1], attrs.get(key));
    }

    public String getName()
    {
        return name;
    }

    public String getStrategyClass()
    {
        return strategyClass;
    }
    
    public int getReplicationFactor()
    {
        return replicationFactor;
    }
    
    public Map<String, String> getStrategyOptions()
    {
        return strategyOptions;
    }
}
