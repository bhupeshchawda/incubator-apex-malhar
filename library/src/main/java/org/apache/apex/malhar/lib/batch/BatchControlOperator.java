package org.apache.apex.malhar.lib.batch;

import com.datatorrent.api.InputOperator;
import com.datatorrent.api.StatsListener;

import java.util.Collection;
import java.util.List;

import org.junit.Assert;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.BaseOperator;

public abstract class BatchControlOperator extends BaseOperator implements InputOperator
{
  private SharedStatsListener statsListener;
  private List<DAG.DAGChangeSet> changeSet;

  public BatchControlOperator()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    Collection<StatsListener> statsListeners = context.getAttributes().get(Context.OperatorContext.STATS_LISTENERS);
    Assert.assertNotNull(statsListeners);
    statsListener = (SharedStatsListener) (statsListeners.size() > 0 ? statsListeners.iterator().next() : null);
    Assert.assertNotNull(statsListener);
  }

  @Override
  public final void emitTuples()
  {
  }

  public void setChangeSet(List<DAG.DAGChangeSet> changeSet)
  {
    this.changeSet = changeSet;
  }
}
