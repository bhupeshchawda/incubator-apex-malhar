package org.apache.apex.malhar.lib.wal;


import java.util.List;
import java.util.Queue;

import org.apache.apex.malhar.lib.state.managed.ManagedState;
import org.apache.apex.malhar.lib.state.managed.ManagedTimeStateImpl;
import org.apache.apex.malhar.lib.state.managed.TimeBucketAssigner;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.netlet.util.Slice;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;

public abstract class OutputManagerImpl<T> implements OutputManager<T>
{
  private static final long DEFAULT_CONSTANT_TIME = 0;

  private ManagedState store = new ManagedTimeStateImpl();
  private long windowId;

  @Override
  public void setup(OperatorContext context)
  {
    // Setup store based on managed state
  }

  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
  }

  @Override
  public void emit(T tuple)
  {
    // Add to store
  }

  public abstract Slice getKey(T tuple);

  @Override
  public T readFromStore()
  {
    return null;
  }

  @Override
  public List<T> readBatchFromStore(int batchsize)
  {
    return null;
  }

  @Override
  public void run()
  {
  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void teardown()
  {
  }

}
