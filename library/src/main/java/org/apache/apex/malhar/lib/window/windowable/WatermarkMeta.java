package org.apache.apex.malhar.lib.window.windowable;

public interface WatermarkMeta
{
  class EofWatermarkMeta implements WatermarkMeta
  {
    private String fileName;

    public EofWatermarkMeta(String fileName)
    {
      this.fileName = fileName;
    }
    public String getFileName()
    {
      return fileName;
    }
  }

  class FinalWatermarkMeta implements WatermarkMeta
  {
  }

  class TimeWatermarkMeta implements WatermarkMeta
  {
  }
}
