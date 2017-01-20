package org.apache.apex.malhar.lib.window.windowable;

import javax.validation.constraints.Min;

public interface WatermarkOption
{
  Type getType();

  enum Type
  {
    FINAL,
    PROCESSING_TIME,
    EVENT_TIME,
    TUPLES,
    EOF
  }

  class TimeWatermarkOption implements WatermarkOption
  {
    @Min(1)
    private long delayMillis;

    public TimeWatermarkOption(long delayMillis)
    {
      this.delayMillis = delayMillis;
    }

    public long getDelayMillis()
    {
      return delayMillis;
    }

    @Override
    public Type getType()
    {
      return Type.PROCESSING_TIME;
    }
  }

  class TupleWatermarkOption implements WatermarkOption
  {
    @Min(1)
    private long numTuples;

    public TupleWatermarkOption(long numTuples)
    {
      this.numTuples = numTuples;
    }

    public long getNumTuples()
    {
      return numTuples;
    }

    @Override
    public Type getType()
    {
      return Type.TUPLES;
    }
  }

  class EofWatermarkOption implements WatermarkOption
  {
    @Override
    public Type getType()
    {
      return Type.EOF;
    }
  }

  class FinalWatermarkOption implements WatermarkOption
  {
    @Override
    public Type getType()
    {
      return Type.FINAL;
    }
  }
}
