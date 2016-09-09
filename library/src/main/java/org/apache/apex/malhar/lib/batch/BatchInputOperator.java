package org.apache.apex.malhar.lib.batch;

import com.datatorrent.common.util.BaseOperator;

public class BatchInputOperator extends BaseOperator implements BatchInput
{
  private long num;
  BatchType batchType;

  @Override
  public void defineBatch(long num, BatchType type)
  {
    this.num = num;
    this.batchType = type;
  }

  @Override
  public void emitTuples()
  {
    
  }
}
