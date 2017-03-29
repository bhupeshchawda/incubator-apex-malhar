package org.apache.apex.malhar.lib.window.windowable;

import org.apache.apex.malhar.lib.window.WatermarkTuple;

/**
 * Generic watermark class
 * Can be used for start and end of batches
 * See {@link BeginFileWatermark} and {@link EndFileWatermark} for file specific uses
 */
public class BatchWatermark implements WatermarkTuple
{
  private long timestamp;
  private DeliveryType deliveryType;
  private boolean isFinalWatermark;

  public BatchWatermark()
  {
  }

  public BatchWatermark(long timestamp, DeliveryType type, boolean isFinalWatermark)
  {
    this.timestamp = timestamp;
    this.deliveryType = type;
    this.isFinalWatermark = isFinalWatermark;
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

  /**
   * Begin of File Watermark
   */
  public static class BeginFileWatermark extends BatchWatermark
  {
    private String fileName;

    private BeginFileWatermark()
    {
      // for kryo
    }

    public BeginFileWatermark(long timestamp, String fileName)
    {
      super(timestamp, DeliveryType.IMMEDIATE, false);
      this.fileName = fileName;
    }

    public String getFileName()
    {
      return fileName;
    }
  }

  /**
   * End of File Watermark
   */
  public static class EndFileWatermark extends BatchWatermark
  {
    private String fileName;

    private EndFileWatermark()
    {
      //for kryo
    }

    public EndFileWatermark(long timestamp, String fileName)
    {
      super(timestamp, DeliveryType.END_WINDOW, false);
      this.fileName = fileName;
    }
  }
}
