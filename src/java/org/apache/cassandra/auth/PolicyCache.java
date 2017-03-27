package org.apache.cassandra.auth;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.PolicyClause;
import org.apache.cassandra.utils.Pair;

import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.apache.cassandra.auth.AbacProxy.listAllPoliciesOn;

/**
 * Created by coleman on 3/27/17.
 */
public class PolicyCache extends AuthCache<Pair<IResource,Permission>, Set<PolicyClause>>
{
    public PolicyCache() // TODO: ABAC Update to not use the permissions cache methods.
    {
        super("PolicyCache",
                DatabaseDescriptor::setPermissionsValidity,
                DatabaseDescriptor::getPermissionsValidity,
                DatabaseDescriptor::setPermissionsUpdateInterval,
                DatabaseDescriptor::getPermissionsUpdateInterval,
                DatabaseDescriptor::setPermissionsCacheMaxEntries,
                DatabaseDescriptor::getPermissionsCacheMaxEntries,
                (p) -> listAllPoliciesOn(p.left, p.right),
                DatabaseDescriptor::isUsingAbac);
    }

    public Set<PolicyClause> getPolicies(IResource resource, Permission perm)
    {
        try
        {
            return get(Pair.create(resource, perm));
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }
}
