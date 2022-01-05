public interface UDFContext
{
    UDTValue newArgUDTValue(String argName);
    UDTValue newArgUDTValue(int argNum);
    UDTValue newReturnUDTValue();
    UDTValue newUDTValue(String udtName);
    TupleValue newArgTupleValue(String argName);
    TupleValue newArgTupleValue(int argNum);
    TupleValue newReturnTupleValue();
    TupleValue newTupleValue(String cqlDefinition);
}
