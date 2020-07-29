package org.apache.cassandra.distributed.upgrade;

import org.apache.cassandra.distributed.shared.Versions;

public class MigrateDropColumns30To311Test extends MigrateDropColumns
{
    public MigrateDropColumns30To311Test()
    {
        super(Versions.Major.v30, Versions.Major.v3X);
    }
}
