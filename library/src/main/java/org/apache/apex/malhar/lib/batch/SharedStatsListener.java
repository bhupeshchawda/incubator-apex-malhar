package org.apache.apex.malhar.lib.batch;

import java.io.Serializable;
import java.util.List;

import com.datatorrent.api.DAG.DAGChangeSet;
import com.datatorrent.api.DAG.OperatorMeta;
import com.datatorrent.api.DAG.StreamMeta;
import com.datatorrent.api.StatsListener;
import com.datatorrent.stram.plan.logical.mod.DAGChangeSetImpl;

public class SharedStatsListener implements StatsListener, StatsListener.ContextAwareStatsListener, Serializable
{
  private static final long serialVersionUID = 1118974904772892490L;

  private List<DAGChangeSetImpl> dagChanges;
  private int index = 0;
  private DAGChangeSetImpl currentDag;
  private transient StatsListenerContext context;
  private String batchControlOperatorName;

  public SharedStatsListener()
  {
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    System.out.println("Num Operators: " + context.numOperatorsInDAG());
    System.out.println("OperatorId: " + stats.getOperatorId());
    if (stats.getOperatorId() == 1) {
      if (context.numOperatorsInDAG() == 1) { // Just Control Operator running
        if (index <= dagChanges.size() - 1) {
          return deployDag(dagChanges.get(index++));
        }
      } else {
        if (context.operatorInactive()) {
          System.out.println("Undeploying DAG");
          return undeployDag(currentDag);
        }
      }
    }
    return null;
  }

  private Response deployDag(DAGChangeSetImpl dag)
  {
    Response response = new Response();
    currentDag = dag;
    response.dagChanges = currentDag;
    System.out.println("Deploying DAG: " + dag.toString());
    System.out.println("Deploying DAG: " + currentDag.toString());
    return response;
  }

  private Response undeployDag(DAGChangeSetImpl dag)
  {
    DAGChangeSetImpl dagChange = new DAGChangeSetImpl();
    for (StreamMeta stream : ((DAGChangeSetImpl)dag).getAllStreams()) {
      dagChange.removeStream(stream.getName());
    }
    for (OperatorMeta operator: ((DAGChangeSetImpl)dag).getAllOperators()) {
      dagChange.removeOperator(operator.getName());
    }
    Response response = new Response();
    response.dagChanges = dagChange;
    return response;
  }

  public List<DAGChangeSetImpl> getDagChanges()
  {
    return dagChanges;
  }

  public void setDagChanges(List<DAGChangeSetImpl> dagChanges)
  {
    this.dagChanges = dagChanges;
  }

  @Override
  public void setContext(StatsListenerContext context)
  {
    System.out.println("Setting context");
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
}
