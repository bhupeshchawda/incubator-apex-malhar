package org.apache.apex.malhar.lib.window.windowable;

import org.apache.apex.api.ControlAwareDefaultOutputPort;
import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;

import com.datatorrent.api.Context;
import com.datatorrent.common.util.BaseOperator;

public class WindowedFileInputOperator extends LineByLineFileInputOperator
{
  private boolean scanned = false;
  private boolean shutdown = false;
  private WatermarkGeneratorImpl watermarkGenerator;

  public final transient ControlAwareDefaultOutputPort output = new ControlAwareDefaultOutputPort();

  @Override
  public void setup(Context.OperatorContext context)
  {
    emitBatchSize = 1;
    super.setup(context);
  }

  @Override
  public void emitTuples()
  {
    if (!watermarkGenerator.isWatermarkReached()) {
      super.emitTuples();
    }
  }

  @Override
  protected void emit(String tuple)
  {
    output.emit(tuple);
    watermarkGenerator.processWatermarkEvents();
  }

  @Override
  protected void scanDirectory()
  {
    if (scanned) {
      watermarkGenerator.processWatermarkEvents(WatermarkOption.Type.FINAL);
      if (watermarkGenerator.isWatermarkReached()) {
        return;
      }
    }
    super.scanDirectory();
    scanned = true;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    if (watermarkGenerator.isWatermarkReached()) {
      WatermarkControlTuple watermark = watermarkGenerator.getWatermark();
      output.emitControl(watermark);
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
