package org.apache.cassandra.stress.generate.values;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.stress.generate.Distribution;
import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.settings.OptionDistribution;

public abstract class Generator<T>
{

    public final String name;
    public final AbstractType<T> type;
    final long salt;
    final Distribution identityDistribution;
    final Distribution sizeDistribution;
    public final Distribution clusteringDistribution;

    public Generator(AbstractType<T> type, GeneratorConfig config, String name)
    {
        this.type = type;
        this.name = name;
        this.salt = config.salt;
        this.identityDistribution = config.getIdentityDistribution(defaultIdentityDistribution());
        this.sizeDistribution = config.getSizeDistribution(defaultSizeDistribution());
        this.clusteringDistribution = config.getClusteringDistribution(defaultClusteringDistribution());
    }

    public void setSeed(long seed)
    {
        identityDistribution.setSeed(seed ^ salt);
        clusteringDistribution.setSeed(seed ^ ~salt);
    }

    public abstract T generate();

    DistributionFactory defaultIdentityDistribution()
    {
        return OptionDistribution.get("uniform(1..100B)");
    }

    DistributionFactory defaultSizeDistribution()
    {
        return OptionDistribution.get("uniform(4..8)");
    }

    DistributionFactory defaultClusteringDistribution()
    {
        return OptionDistribution.get("fixed(1)");
    }
}
