package org.apache.apex.malhar.lib.window.windowable;

import java.util.List;

import org.apache.apex.malhar.lib.state.managed.TimeExtractor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import static org.apache.apex.malhar.lib.window.windowable.WatermarkOption.Type.EVENT_TIME;
import static org.apache.apex.malhar.lib.window.windowable.WatermarkOption.Type.PROCESSING_TIME;
import static org.apache.apex.malhar.lib.window.windowable.WatermarkOption.Type.TUPLES;

public class WatermarkGeneratorImpl<T> implements WatermarkGenerator<T>
{
  // properties
  private List<WatermarkOption> watermarkOptions;
  private TimeExtractor<T> timeExtractor = null;

  // internal
  private long lastTimeMillis = System.currentTimeMillis();
  private long lastNumTuples = 0;
  private String lastEndedFileName;
  private long watermarkDelayMillis;
  private long watermarkNumTuples;
  private boolean nextWatermarkReached = false;
  private boolean finalWatermarkReached = false;

  private void preProcessWatermarkOption()
  {
    for (WatermarkOption option: watermarkOptions) {
      if (option.getType() == WatermarkOption.Type.PROCESSING_TIME) {
        watermarkDelayMillis = ((WatermarkOption.TimeWatermarkOption) option).getDelayMillis();
      } else if (option.getType() == TUPLES) {
        watermarkNumTuples = ((WatermarkOption.TimeWatermarkOption) option).getDelayMillis();
      }
      if (option.getType() == EVENT_TIME) {
        Preconditions.checkArgument(timeExtractor != null);
      }
    }
  }

  @Override
  public List<WatermarkControlTuple> getWatermarks()
  {
    List<WatermarkControlTuple> watermarks = Lists.newArrayList();
    for (WatermarkOption option: watermarkOptions) {
      switch (option.getType()) {
        case PROCESSING_TIME:
          watermarks.add(new WatermarkControlTuple.TimeWatermark(lastTimeMillis));
          break;
        case TUPLES:
          watermarks.add(new WatermarkControlTuple.TupleWatermark(lastNumTuples));
          break;
        case FINAL:
          watermarks.add(new WatermarkControlTuple.FinalWatermark());
          finalWatermarkReached = true;
          break;
        case EOF:
          watermarks.add(new WatermarkControlTuple.EofWatermark(lastEndedFileName));
          break;
        default:
          throw new IllegalArgumentException("Watermark option " + option.getType() + " not supported");
      }
    }
    return watermarks;
  }

  @Override
  public void processTupleEvents(T tuple)
  {
    if (watermarkOptions.contains(TUPLES)) {
      // Update the TUPLES state
    } else if (watermarkOptions.contains(PROCESSING_TIME)) {
      // Update processing time state
    } else if (watermarkOptions.contains(EVENT_TIME)) {
      // Update event time state
    }
  }

  @Override
  public void processEofEvent(String fileName)
  {
    nextWatermarkReached = true;
    lastEndedFileName = fileName;
  }

  @Override
  public void processFinalEvent()
  {
    nextWatermarkReached = true;
    finalWatermarkReached = true;
  }

  public boolean isNextWatermarkReached()
  {
    return nextWatermarkReached;
  }

  @Override
  public boolean isFinalWatermarkReached()
  {
    return finalWatermarkReached;
  }

  public List<WatermarkOption> getWatermarkOptions()
  {
    return watermarkOptions;
  }

  public void setWatermarkOptions(List<WatermarkOption> watermarkOptions)
  {
    this.watermarkOptions = watermarkOptions;
  }
}
