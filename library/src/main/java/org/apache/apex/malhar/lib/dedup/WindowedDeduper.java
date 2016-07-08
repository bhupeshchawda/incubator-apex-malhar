package org.apache.apex.malhar.lib.dedup;

import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.impl.WindowedOperatorImpl;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

public class WindowedDeduper<T> extends WindowedOperatorImpl<T, T, T>
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

  @Override
  public void dropTuple(Tuple<T> input)
  {
    expired.emit(input.getValue());
  }

  @Override
  public void accumulateTuple(Tuple.WindowedTuple<T> tuple)
  {
    for (Window window : tuple.getWindows()) {
      // process each window
      T accum = dataStorage.get(window);
      if (accum == null) {
        output.emit(tuple.getValue());
        dataStorage.put(window, accumulation.accumulate(accum, tuple.getValue()));
      } else {
        duplicates.emit(tuple.getValue());
      }
    }
  }

}
