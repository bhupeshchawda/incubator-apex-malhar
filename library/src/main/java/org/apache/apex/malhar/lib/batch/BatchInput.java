package org.apache.apex.malhar.lib.batch;

import com.datatorrent.api.InputOperator;

public interface BatchInput extends InputOperator
{
  void defineBatch(long num, BatchType type);

  enum BatchType
  {
    TUPLES,
    TIME,
    FILES
  }
}
