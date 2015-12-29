/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.contrib.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Queues;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.util.FieldValueGenerator;
import com.datatorrent.lib.util.FieldValueGenerator.ValueConverter;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Setter;
import com.datatorrent.lib.util.TableInfo;
import com.datatorrent.api.Context.OperatorContext;

/**
 * HBasePOJOInputOperator reads data from a HBase store, converts it to a POJO and puts it on the output port.
 * The read from HBase is asynchronous.
 * @displayName HBase Input Operator
 * @category Input
 * @tags database, nosql, pojo, hbase
 * @since 3.1.0
 */
@Evolving
public class HBasePOJOInputOperator extends HBaseInputOperator<Object> implements Operator.ActivationListener
{
  public static final int DEF_HINT_SCAN_LOOKAHEAD = 2;
  public static final int DEF_QUEUE_SIZE = 1000;
  public static final int DEF_SLEEP_MILLIS = 10;

  private TableInfo<HBaseFieldInfo> tableInfo;
  protected HBaseStore store;
  private String pojoTypeName;
  private String startRow;
  private String endRow;
  private String lastReadRow;
  private Queue<Result> resultQueue = Queues.newLinkedBlockingQueue(DEF_QUEUE_SIZE);
  @AutoMetric
  private long tuplesRead;

  protected transient Class pojoType;
  private transient Setter<Object, String> rowSetter;
  protected transient FieldValueGenerator fieldValueGenerator;
  protected transient BytesValueConverter valueConverter;
  protected transient Scan scan;
  protected transient ResultScanner scanner;
  protected transient Thread readThread;

  public static class BytesValueConverter implements ValueConverter<HBaseFieldInfo>
  {
    @Override
    public Object convertValue( HBaseFieldInfo fieldInfo, Object value)
    {
      return fieldInfo.toValue( (byte[])value );
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      store.connect();
      pojoType = Class.forName(pojoTypeName);
      pojoType.newInstance();   //try create new instance to verify the class.
      rowSetter = PojoUtils.createSetter(pojoType, tableInfo.getRowOrIdExpression(), String.class);
      fieldValueGenerator = HBaseFieldValueGenerator.getHBaseFieldValueGenerator(pojoType, tableInfo.getFieldsInfo() );
      valueConverter = new BytesValueConverter();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void activate(Context context)
  {
    try {
      scan = nextScan();
      scanner = store.getTable().getScanner(scan);
      readThread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            Result result;
            while ((result = scanner.next()) != null) {
              while (!resultQueue.offer(result)) {
                Thread.sleep(DEF_SLEEP_MILLIS);
              }
            }
          } catch (Exception e) {
            logger.debug("Exception in fetching results");
            throw new RuntimeException(e.getMessage());
          } finally {
            scanner.close();
          }
        }
      });
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    readThread.start();
  }

  @Override
  public void deactivate()
  {
    readThread.interrupt();
  }

  @Override
  public void beginWindow(long windowId)
  {
    tuplesRead = 0;
  }

  @Override
  public void teardown()
  {
    try {
      store.disconnect();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void emitTuples()
  {
    try {
      Result result = resultQueue.poll();
      if (result == null) {
        Thread.sleep(DEF_SLEEP_MILLIS);
        return;
      }
      String readRow = Bytes.toString(result.getRow());
      if( readRow.equals( lastReadRow ))
        return;

      Object instance = pojoType.newInstance();
      rowSetter.set(instance, readRow);

       List<Cell> cells = result.listCells();
       for (Cell cell : cells) {
         String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
         String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
        byte[] value = CellUtil.cloneValue(cell);
         ((HBaseFieldValueGenerator)fieldValueGenerator).setColumnValue(instance, columnName, columnFamily, value,
             valueConverter);
      }

      outputPort.emit(instance);
      tuplesRead++;
      lastReadRow = readRow;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected Scan nextScan()
  {
    Scan scan;
    if(lastReadRow==null && startRow==null )
      scan = new Scan();
    else if(endRow == null) {
      scan = new Scan(Bytes.toBytes(lastReadRow == null ? startRow : lastReadRow));
    }
    else {
      scan = new Scan(Bytes.toBytes(lastReadRow == null ? startRow : lastReadRow), Bytes.toBytes(endRow));
    }
    for (HBaseFieldInfo field : tableInfo.getFieldsInfo()) {
      scan.addColumn(Bytes.toBytes(field.getFamilyName()), Bytes.toBytes(field.getColumnName()));
    }
    scan.setAttribute(Scan.HINT_LOOKAHEAD, Bytes.toBytes(DEF_HINT_SCAN_LOOKAHEAD));
    return scan;
  }

  public HBaseStore getStore()
  {
    return store;
  }
  public void setStore(HBaseStore store)
  {
    this.store = store;
  }

  public TableInfo<HBaseFieldInfo> getTableInfo()
  {
    return tableInfo;
  }

  public void setTableInfo(TableInfo<HBaseFieldInfo> tableInfo)
  {
    this.tableInfo = tableInfo;
  }

  public String getPojoTypeName()
  {
    return pojoTypeName;
  }

  public void setPojoTypeName(String pojoTypeName)
  {
    this.pojoTypeName = pojoTypeName;
  }

  public String getStartRow()
  {
    return startRow;
  }

  public void setStartRow(String startRow)
  {
    this.startRow = startRow;
  }

  public String getEndRow()
  {
    return endRow;
  }

  public void setEndRow(String endRow)
  {
    this.endRow = endRow;
  }

  private static final Logger logger = LoggerFactory.getLogger(HBasePOJOInputOperator.class);

}
