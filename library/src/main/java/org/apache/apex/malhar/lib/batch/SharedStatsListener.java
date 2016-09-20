package org.apache.apex.malhar.lib.batch;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;

import com.datatorrent.api.Operator;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.DAG.DAGChangeSet;
import com.datatorrent.api.StatsListener.OperatorResponse;
import com.datatorrent.stram.plan.logical.mod.DAGChangeSetImpl;
import com.google.common.collect.Lists;

public class SharedStatsListener implements StatsListener, StatsListener.ContextAwareStatsListener
{
  private List<DAGChangeSet> dagChanges;
  private DAGChangeSet currentDag;
  private List<DAGChangeSet> changesApplied;
  private StatsListenerContext context;
  private String batchControlOperatorName;

  public SharedStatsListener()
  {
    changesApplied = Lists.newArrayList();
  }

  public void setup()
  {
    Assert.assertNotNull(dagChanges);
    dagChanges = getDagChanges();
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    int id = stats.getOperatorId();
    if (context.getOperatorName(id).equals(batchControlOperatorName)) {
      if (changesApplied.isEmpty() && dagChanges.size() > 0) {
        applyNextDagChange();
      }
    } else {
      // Some operator which is part of current DAG
      String opName = context.getOperatorName(id);
      if (opName == null ){
        if (dagChanges.size() > 0) {
          applyNextDagChange();
        }
      }
      
    }
    return null;
  }

  private Response applyNextDagChange()
  {
    Response response = new Response();
    if (dagChanges.size() > 0) {
      currentDag = dagChanges.remove(0);
      changesApplied.add(currentDag);
      response.dagChanges = currentDag;
      return response;
    } else { // Done with all Dag Changes
      OperatorRequest request = new ShutdownOperatorRequest();
      response.operatorRequests = Lists.newArrayList(request);
      return response;
    }
  }

  public List<DAGChangeSet> getDagChanges()
  {
    return dagChanges;
  }

  public void setDagChanges(List<DAGChangeSet> dagChanges)
  {
    this.dagChanges = dagChanges;
  }

  @Override
  public void setContext(StatsListenerContext context)
  {
    this.context = context;
  }

  public void setBatchControlOperatorName(String batchControlOperatorName)
  {
    this.batchControlOperatorName = batchControlOperatorName;
  }

  public String getBatchControlOperatorName()
  {
    return batchControlOperatorName;
  }

  public static class ShutdownOperatorRequest implements OperatorRequest
  {
    @Override
    public OperatorResponse execute(Operator operator, int operatorId, long windowId) throws IOException
    {
      operator.teardown();
      return null;
    }
  }
}
