/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.guardrails;

public abstract class CustomGuardrailsFactory
{
    public static GuardrailsFactory make(String customImpl)
    {
        try
        {
            return (GuardrailsFactory) Class.forName(customImpl).newInstance();
        }
        catch (Throwable ex)
        {
            throw new IllegalStateException("Unknown guardrails factory: " + customImpl);
        }
    }
}
