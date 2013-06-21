package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.net.MessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sidekick helper for snitches that want to reconnect from one IP addr for a node to another.
 * Typically, this is for situations like EC2 where a node will have a public address and a private address,
 * where we connect on the public, discover the private, and reconnect on the private.
 */
public class ReconnectableSnitchHelper implements IEndpointStateChangeSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(ReconnectableSnitchHelper.class);
    private final IEndpointSnitch snitch;
    private final String localDc;
    private final boolean preferLocal;

    public ReconnectableSnitchHelper(IEndpointSnitch snitch, String localDc, boolean preferLocal)
    {
        this.snitch = snitch;
        this.localDc = localDc;
        this.preferLocal = preferLocal;
    }

    private void reconnect(InetAddress publicAddress, VersionedValue localAddressValue)
    {
        try
        {
            reconnect(publicAddress, InetAddress.getByName(localAddressValue.value));
        }
        catch (UnknownHostException e)
        {
            logger.error("Error in getting the IP address resolved: ", e);
        }
    }

    private void reconnect(InetAddress publicAddress, InetAddress localAddress)
    {
        if (snitch.getDatacenter(publicAddress).equals(localDc)
                && MessagingService.instance().getVersion(publicAddress) == MessagingService.current_version
                && !MessagingService.instance().getConnectionPool(publicAddress).endPoint().equals(localAddress))
        {
            MessagingService.instance().getConnectionPool(publicAddress).reset(localAddress);
            logger.debug(String.format("Intiated reconnect to an Internal IP %s for the %s", localAddress, publicAddress));
        }
    }

    public void onJoin(InetAddress endpoint, EndpointState epState)
    {
        if (preferLocal && epState.getApplicationState(ApplicationState.INTERNAL_IP) != null)
            reconnect(endpoint, epState.getApplicationState(ApplicationState.INTERNAL_IP));
    }

    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
    {
        if (preferLocal && state == ApplicationState.INTERNAL_IP)
            reconnect(endpoint, value);
    }

    public void onAlive(InetAddress endpoint, EndpointState state)
    {
        if (preferLocal && state.getApplicationState(ApplicationState.INTERNAL_IP) != null)
            reconnect(endpoint, state.getApplicationState(ApplicationState.INTERNAL_IP));
    }

    public void onDead(InetAddress endpoint, EndpointState state)
    {
        // do nothing.
    }

    public void onRemove(InetAddress endpoint)
    {
        // do nothing.
    }

    public void onRestart(InetAddress endpoint, EndpointState state)
    {
        // do nothing.
    }
}
