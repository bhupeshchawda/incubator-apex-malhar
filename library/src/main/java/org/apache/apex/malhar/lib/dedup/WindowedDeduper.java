package org.apache.apex.malhar.lib.dedup;

import java.util.ArrayList;
import java.util.List;

import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.impl.WindowedOperatorImpl;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

public abstract class WindowedDeduper<T> extends WindowedOperatorImpl<T, List<T>, T>
{

  /**
   * The output port on which deduped events are emitted.
   */
  public final transient DefaultOutputPort<T> output = new DefaultOutputPort<T>();

  /**
   * The output port on which duplicate events are emitted.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<T> duplicates = new DefaultOutputPort<T>();

  /**
   * The output port on which expired events are emitted.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<T> expired = new DefaultOutputPort<T>();

  protected abstract Object getKey(T tuple);

  @Override
  public void dropTuple(Tuple<T> input)
  {
    expired.emit(input.getValue());
  }

  @Override
  public void accumulateTuple(Tuple.WindowedTuple<T> tuple)
  {
    for (Window window : tuple.getWindows()) { // There will be exactly one window
      // process each window
      List<T> accum = dataStorage.get(window);
      if (accum == null) {
        accum = new ArrayList<T>();
        output.emit(tuple.getValue());
        dataStorage.put(window, accumulation.accumulate(accum, tuple.getValue()));
      } else {
        Object tupleKey = getKey(tuple.getValue());
        for (T key: accum) {
          if (getKey(key).equals(tupleKey)) {
            duplicates.emit(tuple.getValue());
            return;
          }
        }
        output.emit(tuple.getValue());
        dataStorage.put(window, accumulation.accumulate(accum, tuple.getValue()));
      }
    }
  }
}
