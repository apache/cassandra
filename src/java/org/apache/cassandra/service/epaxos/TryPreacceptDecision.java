package org.apache.cassandra.service.epaxos;

/**
 * Result of nodes running TryPreaccepts. If a potential
 * conflict is not committed, the contended status will be
 * returned
 */
public enum TryPreacceptDecision
{
    ACCEPTED(0),
    REJECTED(1),
    CONTENDED(2);

    TryPreacceptDecision(int expectedOrdinal)
    {
        assert ordinal() == expectedOrdinal;
    }
}
