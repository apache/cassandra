package io.teknek.arizona.transform;


import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.thrift.SlicePredicate;

public class FunctionalTransform {

  private ColumnFamily columnFamily;
  private SlicePredicate slicePredicate;
  private CFMetaData cfm;
  private Transformer transformer;
  
  public FunctionalTransform(SlicePredicate predicate, ColumnFamily columnFamily, CFMetaData cfm, Transformer trans){
    this.columnFamily = columnFamily;
    this.slicePredicate = predicate;
    this.cfm = cfm;
    this.transformer = trans;
  }

  public ColumnFamily getColumnFamily() {
    return columnFamily;
  }

  public void setColumnFamily(ColumnFamily columnFamily) {
    this.columnFamily = columnFamily;
  }

  public SlicePredicate getSlicePredicate() {
    return slicePredicate;
  }

  public void setSlicePredicate(SlicePredicate slicePredicate) {
    this.slicePredicate = slicePredicate;
  }

  public CFMetaData getCfm() {
    return cfm;
  }

  public void setCfm(CFMetaData cfm) {
    this.cfm = cfm;
  }

  public Transformer getTransformer() {
    return transformer;
  }

  public void setTransformer(Transformer transformer) {
    this.transformer = transformer;
  }    
  
}