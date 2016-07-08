package org.apache.apex.malhar.lib.dedup;

import org.apache.apex.malhar.lib.window.Accumulation;

public class DedupAccumulation<T> implements Accumulation<T, T, T>
{

  @Override
  public T defaultAccumulatedValue()
  {
    return null;
  }

  @Override
  public T accumulate(T accumulatedValue, T input)
  {
    return input;
  }

  @Override
  public T merge(T accumulatedValue1, T accumulatedValue2)
  {
    throw new UnsupportedOperationException("Merge not supported for non-session windows");
  }

  @Override
  public T getOutput(T accumulatedValue)
  {
    return accumulatedValue;
  }

  @Override
  public T getRetraction(T accumulatedValue)
  {
    throw new UnsupportedOperationException("Retraction not needed for Dedup");
  }
  
}
