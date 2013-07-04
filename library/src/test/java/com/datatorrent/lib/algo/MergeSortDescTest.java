/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.lib.algo.MergeSortDesc;
import com.datatorrent.lib.testbench.CollectorTestSink;
import java.util.ArrayList;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.algo.MergeSortDesc}<p>
 */
public class MergeSortDescTest
{
  private static Logger log = LoggerFactory.getLogger(MergeSortDescTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new MergeSortDesc<Integer>(), "Integer");
    testNodeProcessingSchema(new MergeSortDesc<Double>(), "Double");
    testNodeProcessingSchema(new MergeSortDesc<Float>(), "Float");
    testNodeProcessingSchema(new MergeSortDesc<String>(), "String");
  }

  public void testNodeProcessingSchema(MergeSortDesc oper, String debug)
  {
    //FirstN<String,Float> aoper = new FirstN<String,Float>();
    CollectorTestSink sortSink = new CollectorTestSink();
    CollectorTestSink hashSink = new CollectorTestSink();
    oper.sort.setSink(sortSink);
    oper.sorthash.setSink(hashSink);

    ArrayList input = new ArrayList();

    oper.beginWindow(0);

    input.clear();
    input.add(2);
    oper.data.process(input);

    input.clear();
    input.add(20);
    oper.data.process(input);

    input.clear();
    input.add(1000);
    input.add(5);
    input.add(20);
    input.add(33);
    input.add(33);
    input.add(34);
    oper.data.process(input);

    input.clear();
    input.add(34);
    input.add(1001);
    input.add(6);
    input.add(1);
    input.add(33);
    input.add(9);
    oper.data.process(input);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 1, hashSink.collectedTuples.size());
    HashMap map = (HashMap) hashSink.collectedTuples.get(0);
    input = (ArrayList) sortSink.collectedTuples.get(0);
    for (Object o: input) {
     log.debug(String.format("%s : %s", o.toString(), map.get(o).toString()));
    }
    log.debug(String.format("Tested %s type with %d tuples and %d uniques\n", debug, input.size(), map.size()));
  }
}
