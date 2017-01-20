package org.apache.apex.malhar.lib.window.windowable;

public interface WatermarkGenerator<T>
{
  void emitWatermarkAtEndWindow();

  void setWatermarkOption(WatermarkOption watermarkOption);

  void processTupleForWatermark(T tuple);
}
