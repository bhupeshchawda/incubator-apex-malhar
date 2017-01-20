package org.apache.apex.malhar.lib.window.windowable;

import org.apache.apex.api.ControlAwareDefaultOutputPort;
import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;

import com.datatorrent.api.Context;
import com.datatorrent.common.util.BaseOperator;

public class WindowedFileInputOperator extends LineByLineFileInputOperator implements WatermarkGenerator<String>
{
  private WatermarkOption watermarkOption;
  private boolean scanned = false;
  private boolean suspend = false;
  private boolean shutdown = false;
  private boolean emitWatermark = false;
  private long lastTimeMillis = System.currentTimeMillis();
  private long lastNumTuples = 0;
  private long watermarkDelayMillis;
  private long watermarkNumTuples;

  public final transient ControlAwareDefaultOutputPort output = new ControlAwareDefaultOutputPort();

  @Override
  public void setup(Context.OperatorContext context)
  {
    emitBatchSize = 1;
    super.setup(context);
    preProcessWatermarkOption();
  }

  private void preProcessWatermarkOption()
  {
    switch (watermarkOption.getType()) {
      case TIME:
        watermarkDelayMillis = ((WatermarkOption.TimeWatermarkOption) watermarkOption).getDelayMillis();
        break;
      case TUPLES:
        watermarkNumTuples = ((WatermarkOption.TimeWatermarkOption) watermarkOption).getDelayMillis();
        break;
    }
  }

  @Override
  public void emitTuples()
  {
    if (!suspend) {
      super.emitTuples();
    }
  }

  @Override
  protected void emit(String tuple)
  {
    output.emit(tuple);
    processTupleForWatermark(tuple);
  }

  @Override
  protected void scanDirectory()
  {
    if (scanned && watermarkOption.getType() == WatermarkOption.Type.FINAL) {
      suspend = true;
      emitWatermark = true;
    }
    super.scanDirectory();
    scanned = true;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    if (emitWatermark) {
      emitWatermarkAtEndWindow();
      emitWatermark = false;
    }
  }

  @Override
  public void emitWatermarkAtEndWindow()
  {
    switch (watermarkOption.getType()) {
      case FINAL:
        output.emitControl(new WatermarkControlTuple.FinalWatermark());
        shutdown = true;
        break;
      case TIME:
        suspend = false;
        break;
      case TUPLES:
        suspend = false;
        break;
      default:
        throw new IllegalArgumentException("Watermark option " + watermarkOption.getType() + " not supported");
    }
  }

  @Override
  public void setWatermarkOption(WatermarkOption watermarkOption)
  {
    this.watermarkOption = watermarkOption;
  }

  @Override
  public void processTupleForWatermark(String tuple)
  {
    if (watermarkOption.getType() == WatermarkOption.Type.TIME
        && System.currentTimeMillis() - lastTimeMillis >= watermarkDelayMillis) {
      suspend = true;
      emitWatermark = true;
    } else if (watermarkOption.getType() == WatermarkOption.Type.TIME && ++lastNumTuples >= watermarkNumTuples) {
      suspend = true;
      emitWatermark = true;
    }
  }

  @Override
  public void committed(long windowId)
  {
    super.committed(windowId);
    if (shutdown) {
      BaseOperator.shutdown();
    }
  }
}
