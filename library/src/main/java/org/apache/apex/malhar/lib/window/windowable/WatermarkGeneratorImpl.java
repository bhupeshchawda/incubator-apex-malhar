package org.apache.apex.malhar.lib.window.windowable;

import java.util.List;

import com.google.common.collect.Lists;

import static org.apache.apex.malhar.lib.window.windowable.WatermarkOption.Type.TUPLES;

public class WatermarkGeneratorImpl<T> implements WatermarkGenerator<T>
{
  private List<WatermarkOption> watermarkOptions;

  private long lastTimeMillis = System.currentTimeMillis();
  private long lastNumTuples = 0;
  private String lastEndedFileName;
  private long watermarkDelayMillis;
  private long watermarkNumTuples;
  private boolean watermarkReached = false;

  private void preProcessWatermarkOption()
  {
    for (WatermarkOption option: watermarkOptions) {
      if (option.getType() == WatermarkOption.Type.PROCESSING_TIME) {
        watermarkDelayMillis = ((WatermarkOption.TimeWatermarkOption) option).getDelayMillis();
      } else if (option.getType() == TUPLES) {
        watermarkNumTuples = ((WatermarkOption.TimeWatermarkOption) option).getDelayMillis();
      }
    }
  }

  @Override
  public List<WatermarkControlTuple> getWatermark()
  {
    List<WatermarkControlTuple> watermarks = Lists.newArrayList();
    for (WatermarkOption option: watermarkOptions) {
      switch (option.getType()) {
        case PROCESSING_TIME:
          watermarks.add(new WatermarkControlTuple.TimeWatermark(lastTimeMillis));
        case TUPLES:
          watermarks.add(new WatermarkControlTuple.TupleWatermark(lastNumTuples));
        case FINAL:
          watermarks.add(new WatermarkControlTuple.FinalWatermark());
        case EOF:
          watermarks.add(new WatermarkControlTuple.EofWatermark(lastEndedFileName));
        default:
          throw new IllegalArgumentException("Watermark option " + option.getType() + " not supported");
      }
    }
  }

  @Override
  public void processWatermarkEvents(WatermarkOption.Type type, WatermarkMeta meta)
  {
    if (type == WatermarkOption.Type.FINAL) {
      watermarkReached = true;
    }
    if (type == WatermarkOption.Type.EOF ) {
      watermarkReached = true;
      lastEndedFileName = ((WatermarkMeta.EofWatermarkMeta)meta).getFileName();
    }
    if (System.currentTimeMillis() - lastTimeMillis >= watermarkDelayMillis) {
      watermarkReached = true;
    }
    if (++lastNumTuples >= watermarkNumTuples) {
      watermarkReached = true;
    }
  }

  public boolean isWatermarkReached()
  {
    return watermarkReached;
  }


}
