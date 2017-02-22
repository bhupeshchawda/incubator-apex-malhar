package org.apache.apex.malhar.lib.window.windowable;

import org.apache.apex.malhar.lib.window.WatermarkTuple;

public class FileWatermark implements WatermarkTuple
{
  private long timestamp;
  private DeliveryType deliveryType;
  private String fileName;
  private int operatorId;

  public FileWatermark()
  {
  }

  public FileWatermark(int operatorId, long timestamp, DeliveryType type, String fileName)
  {
    this.operatorId = operatorId;
    this.timestamp = timestamp;
    this.deliveryType = type;
    this.fileName = fileName;
  }

  @Override
  public long getTimestamp()
  {
    return timestamp;
  }

  @Override
  public DeliveryType getDeliveryType()
  {
    return deliveryType;
  }

  public String getFileName()
  {
    return fileName;
  }

  public static class BeginFileWatermark extends FileWatermark
  {
    public BeginFileWatermark()
    {
    }
    public BeginFileWatermark(int opId, long timestamp, String fileName)
    {
      super(opId, timestamp, DeliveryType.IMMEDIATE, fileName);
    }
  }

  public static class EndFileWatermark extends FileWatermark
  {
    public EndFileWatermark()
    {
    }
    public EndFileWatermark(int opId, long timestamp, String fileName)
    {
      super(opId, timestamp, DeliveryType.END_WINDOW, fileName);
    }
  }
}
