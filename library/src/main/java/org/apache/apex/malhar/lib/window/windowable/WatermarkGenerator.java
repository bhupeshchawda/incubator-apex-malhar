package org.apache.apex.malhar.lib.window.windowable;

public interface WatermarkGenerator<T>
{
  WatermarkControlTuple getWatermark();

  void setWatermarkOption(WatermarkOption watermarkOption);

  void processWatermarkEvents(WatermarkOption.Type type, T tuple);
}
