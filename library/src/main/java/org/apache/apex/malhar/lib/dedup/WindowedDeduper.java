package org.apache.apex.malhar.lib.dedup;

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.Tuple.WindowedTuple;
import org.apache.apex.malhar.lib.window.impl.KeyedWindowedOperatorImpl;

import com.datatorrent.lib.util.KeyValPair;

public class WindowedDeduper<T> extends KeyedWindowedOperatorImpl<Long, T, T, T>
{
  @Override
  public void accumulateTuple(WindowedTuple<KeyValPair<Long, T>> tuple)
  {
    super.accumulateTuple(tuple);
  }

  public static class DedupAccumulation<T> implements Accumulation<T, T, T>
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
}
