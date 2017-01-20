package org.apache.apex.malhar.lib.window.windowable;

import org.apache.apex.api.UserDefinedControlTuple;

public interface WatermarkControlTuple extends UserDefinedControlTuple
{
  WatermarkOption.Type getWatermarkType();

  class FinalWatermark implements WatermarkControlTuple
  {
    public FinalWatermark()
    {
    }

    @Override
    public WatermarkOption.Type getWatermarkType()
    {
      return WatermarkOption.Type.FINAL;
    }
  }

  class TimeWatermark implements WatermarkControlTuple
  {
    private long watermarkMillis;

    public TimeWatermark(long watermarkMillis)
    {
      this.watermarkMillis = watermarkMillis;
    }

    @Override
    public WatermarkOption.Type getWatermarkType()
    {
      return WatermarkOption.Type.TIME;
    }
  }

  class TupleWatermark implements WatermarkControlTuple
  {
    private long watermarkNumTuples;

    public TupleWatermark(long watermarkNumTuples)
    {
      this.watermarkNumTuples = watermarkNumTuples;
    }

    @Override
    public WatermarkOption.Type getWatermarkType()
    {
      return WatermarkOption.Type.TUPLES;
    }
  }

  class EofWatermark implements WatermarkControlTuple
  {
    private long numFilesEnded;

    public EofWatermark(long numFilesEnded)
    {
      this.numFilesEnded = numFilesEnded;
    }

    @Override
    public WatermarkOption.Type getWatermarkType()
    {
      return WatermarkOption.Type.EOF;
    }
  }
}
