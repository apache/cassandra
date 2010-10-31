package org.apache.cassandra.cql;

import java.util.ArrayList;
import java.util.List;

/**
 * SelectExpressions encapsulate all of the predicates of a SELECT query.
 * 
 * @author eevans
 *
 */
public class SelectExpression
{
    private Predicates keys = new Predicates();
    private Predicates columns = new Predicates();
    
    public SelectExpression(Relation firstRelation)
    {
        and(firstRelation);
    }
    
    public void and(Relation relation)
    {
        if (relation.isKey())
        {
            if (relation.type.equals(RelationType.EQ))
                keys.addTerm(relation.value);
            else if ((relation.type.equals(RelationType.GT) || relation.type.equals(RelationType.GTE)))
                keys.setStart(relation.value);
            else if ((relation.type.equals(RelationType.LT) || relation.type.equals(RelationType.LTE)))
                keys.setFinish(relation.value);
        }
        else    // It's a column
        {
            if (relation.type.equals(RelationType.EQ))
                columns.addTerm(relation.value);
            else if ((relation.type.equals(RelationType.GT) || relation.type.equals(RelationType.GTE)))
                columns.setStart(relation.value);
            else if ((relation.type.equals(RelationType.LT) || relation.type.equals(RelationType.LTE)))
                columns.setFinish(relation.value);
        }
    }
    
    public Predicates getKeyPredicates()
    {
        return keys;
    }
    
    public Predicates getColumnPredicates()
    {
        return columns;
    }
}

class Predicates
{
    private boolean initialized = false;
    private List<Term> names = new ArrayList<Term>();
    private Term start, finish;
    private boolean isRange = false;
    
    Term getStart()
    {
        return start == null ? new Term() : start;
    }
    
    void setStart(Term start)
    {
        // FIXME: propagate a proper exception
        if (initialized && (!isRange()))
            throw new RuntimeException("You cannot combine discreet names and range operators.");
        
        initialized = true;
        isRange = true;
        this.start = start;
    }
    
    Term getFinish()
    {
        return finish == null ? new Term() : finish;
    }
    
    void setFinish(Term finish)
    {
        // FIXME: propagate a proper exception
        if (initialized && (!isRange()))
            throw new RuntimeException("You cannot combine discreet names and range operators.");
        
        initialized = true;
        isRange = true;
        this.finish = finish;
    }
    
    List<Term> getTerms()
    {
        return names;
    }
    
    void addTerm(Term name)
    {
        // FIXME: propagate a proper exception
        if (initialized && (isRange()))
            throw new RuntimeException("You cannot combine discreet names and range operators.");
        
        initialized = true;
        isRange = false;
        names.add(name);
    }
    
    boolean isRange()
    {
        return isRange;
    }
}
