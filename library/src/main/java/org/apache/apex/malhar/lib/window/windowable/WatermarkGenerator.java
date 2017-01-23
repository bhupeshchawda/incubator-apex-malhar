package org.apache.apex.malhar.lib.window.windowable;

import java.util.List;

public interface WatermarkGenerator<T>
{
  List<WatermarkControlTuple> getWatermarks();

  void processTupleEvents(T tuple);

  void processEofEvent(String fileName);

  void processFinalEvent();

  boolean isNextWatermarkReached();

  boolean isFinalWatermarkReached();
}
