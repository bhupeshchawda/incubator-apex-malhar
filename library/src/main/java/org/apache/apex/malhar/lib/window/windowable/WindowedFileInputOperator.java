package org.apache.apex.malhar.lib.window.windowable;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.apex.api.ControlAwareDefaultOutputPort;
import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context;
import com.datatorrent.common.util.BaseOperator;

public class WindowedFileInputOperator extends LineByLineFileInputOperator
{
  private boolean scanned = false;
  private boolean shutdown = false;
  private String currentFileName = "";
  @NotNull
  private WatermarkGeneratorImpl watermarkGenerator;

  public final transient ControlAwareDefaultOutputPort output = new ControlAwareDefaultOutputPort();

  @Override
  public void setup(Context.OperatorContext context)
  {
    emitBatchSize = 1;
    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    if (shutdown) {
      BaseOperator.shutdown();
    }
  }

  @Override
  protected InputStream openFile(Path path) throws IOException
  {
    currentFileName = path.toString();
    return super.openFile(path);
  }

  @Override
  public void emitTuples()
  {
    if (!watermarkGenerator.isNextWatermarkReached()) {
      super.emitTuples();
    }
  }

  @Override
  protected void emit(String tuple)
  {
    output.emit(tuple);
    watermarkGenerator.processTupleEvents(tuple);
  }

  @Override
  protected void scanDirectory()
  {
    if (scanned) {
      watermarkGenerator.processEofEvent(currentFileName);
      if (watermarkGenerator.isNextWatermarkReached()) {
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
    if (watermarkGenerator.isNextWatermarkReached()) {
      List<WatermarkControlTuple> watermarks = watermarkGenerator.getWatermarks();
      for (WatermarkControlTuple watermark: watermarks) {
        output.emitControl(watermark);
      }
    }
  }

  @Override
  public void committed(long windowId)
  {
    super.committed(windowId);
    if (watermarkGenerator.isFinalWatermarkReached()) {
      shutdown = true;
    }
  }

  public void setWatermarkGenerator(WatermarkGeneratorImpl watermarkGenerator)
  {
    this.watermarkGenerator = watermarkGenerator;
  }
}
