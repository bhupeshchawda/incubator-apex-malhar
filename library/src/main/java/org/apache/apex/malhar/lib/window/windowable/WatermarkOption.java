package org.apache.apex.malhar.lib.window.windowable;

import javax.validation.constraints.Min;

public class WatermarkOption
{
  Type type;

  public WatermarkOption(Type type)
  {
    this.type = type;
  }

  public Type getType()
  {
    return type;
  }

  public enum Type
  {
    FINAL,
    TIME,
    TUPLES,
    EOF
  }

  class TimeWatermarkOption extends WatermarkOption
  {
    @Min(1)
    private long delayMillis;

    public TimeWatermarkOption(Type type)
    {
      super(type);
    }

    public void setDelayMillis(long delayMillis)
    {
      this.delayMillis = delayMillis;
    }

    public long getDelayMillis()
    {
      return delayMillis;
    }
  }

  class TupleWatermarkOption extends WatermarkOption
  {
    @Min(1)
    private long numTuples;

    public TupleWatermarkOption(Type type)
    {
      super(type);
    }

    public long getNumTuples()
    {
      return numTuples;
    }

    public void setNumTuples(long numTuples)
    {
      this.numTuples = numTuples;
    }
  }


}
