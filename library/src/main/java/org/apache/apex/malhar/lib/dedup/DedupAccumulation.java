package org.apache.apex.malhar.lib.dedup;

import java.util.List;

import org.apache.apex.malhar.lib.window.Accumulation;

public class DedupAccumulation<T> implements Accumulation<T, List<T>, T>
{

  @Override
  public List<T> defaultAccumulatedValue()
  {
    return null;
  }

  @Override
  public List<T> accumulate(List<T> accumulatedValue, T input)
  {
    accumulatedValue.add(input);
    return accumulatedValue;
  }

  @Override
  public List<T> merge(List<T> accumulatedValue1, List<T> accumulatedValue2)
  {
    throw new UnsupportedOperationException("Merge not supported for non-session windows");
  }

  @Override
  public T getOutput(List<T> accumulatedValue)
  {
    return accumulatedValue.get(accumulatedValue.size()-1);
  }

  @Override
  public T getRetraction(T accumulatedValue)
  {
    throw new UnsupportedOperationException("Retraction not needed for Dedup");
  }
  
}
