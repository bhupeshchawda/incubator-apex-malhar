package org.apache.apex.malhar.lib.wal;

import java.util.List;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.api.Operator;

public interface OutputManager<T> extends Component<Context.OperatorContext>, Runnable
{
  void beginWindow(long windowId);

  void endWindow();

  void emit(T tuple);

  boolean processPending();

  T readFromStore();

  List<T> readBatchFromStore(int batchsize);
}
