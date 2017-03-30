package org.apache.cassandra.cql3;

import java.io.Serializable;

/**
 * Created by coleman on 3/27/17.
 */
public class PolicyClause implements Serializable
{
    private String attribute;
    private Operator operator;
    private ColumnIdentifier colId;
    private Constants constant;

    public PolicyClause(String attribute, Operator op, ColumnIdentifier colId)
    {
        this.attribute = attribute;
        this.operator = op;
        this.colId = colId;
    }

    public PolicyClause(String attribute, Operator op, Constants constants)
    {
        this.attribute = attribute;
        this.operator = op;
        this.constant = constants;
    }

    public PolicyClause(String attribute, ColumnIdentifier colId)
    {
        this.attribute = attribute;
        this.colId = colId;
    }

    @Override
    public String toString()
    {
        if(operator == Operator.CONTAINS)
        {
            return String.format("Column value %s contains attribute %s value.", colId.toCQLString(), attribute);
        }
        else if(operator == null)
        {
            return String.format("Attribute %s value is in column %s", attribute, colId.toCQLString());
        }
        else if(constant != null)
        {
            return String.format("Attribute %s value is not null", attribute);
        }
        else
        {
            return String.format("Column value %s is %d attribute %s value", colId.toCQLString(), operator.getValue(), attribute);
        }
    }

    public String generateWhereClause()
    {
        return null; // TODO: ABAC produce the clause that will be added to the Query for this rule.
    }
}
