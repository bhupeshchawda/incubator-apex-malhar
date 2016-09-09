package org.apache.apex.malhar.lib.batch;

import java.util.List;

import com.datatorrent.api.DAG.DAGChangeSet;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.plan.logical.mod.DAGChangeSetImpl;
import com.google.common.collect.Lists;

public abstract class BatchControlOperator extends BaseOperator implements InputOperator, StatsListener.ContextAwareStatsListener
{
  private List<DAGChangeSet> dagChanges;
  private DAGChangeSet currentDag;
  private List<DAGChangeSet> changesApplied;
  private StatsListenerContext statsContext;

  public BatchControlOperator()
  {
    dagChanges = Lists.newArrayList();
    changesApplied = Lists.newArrayList();
  }

  @Override
  public void setup(OperatorContext context)
  {
    dagChanges = getDAGChangeSequence();
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    if (changesApplied.size() == 0) {
      return applyNextDagChange();
    } else {
      if (stats.getLastWindowedStats().size() <= 1) {
        return applyNextDagChange();
      }
    }
    return null;
  }

  private Response applyNextDagChange()
  {
    if (currentDag != null) {
      changesApplied.add(currentDag);
    }
    if (dagChanges.size() > 0) {
      currentDag = dagChanges.remove(0);
      Response response = new Response();
      response.dagChanges = currentDag;
      return response;
    } else {
      // Done with all Dag Changes
      Response response = new Response();
      DAGChangeSetImpl change = new DAGChangeSetImpl();
      change.removeOperator(this);
      response.dagChanges = change;
      return response;
    }
  }

  @Override
  public void setContext(StatsListenerContext context)
  {
    statsContext = context;
  }

  @Override
  public final void emitTuples()
  {
  }

  public abstract List<DAGChangeSet> getDAGChangeSequence();
}
