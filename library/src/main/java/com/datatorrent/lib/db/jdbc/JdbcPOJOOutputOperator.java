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
package com.datatorrent.lib.db.jdbc;

import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.DefaultOutputPort;

/**
 * <p>
 * JdbcPOJOOutputOperator class.
 * </p>
 * A concrete implementation of AbstractJdbcPOJOOutputOperator which takes in any POJO. This class accepts a
 * parameterized sql query which has "?" as placeholders for values which are dynamically substituted from the incoming
 * POJO.
 *
 * @displayName Jdbc Output Operator
 * @category Output
 * @tags database, sql, pojo, jdbc
 */
@Evolving
public class JdbcPOJOOutputOperator extends AbstractJdbcPOJOOutputOperator
{
  /**
   * A parameterized sql query to be executed using data from the incoming POJO.
   * Example: "update testTable set id = ? where name = ?"
   * The "?"s will be substituted from the incoming POJO using the mapping from the configured JdbcFieldInfo objects
   */
  private String sqlQuery;

  @AutoMetric
  private long numRecordsWritten;
  @AutoMetric
  private long numErrorRecords;

  public transient DefaultOutputPort<Object> error = new DefaultOutputPort<>();

  public JdbcPOJOOutputOperator()
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    numRecordsWritten = 0;
    numErrorRecords = 0;
  }

  @Override
  public void processTuple(Object tuple)
  {
    try {
      super.processTuple(tuple);
      numRecordsWritten++;
    } catch (RuntimeException e) {
      numErrorRecords++;
      error.emit(tuple);
    }
  }

  @Override
  protected String getUpdateCommand()
  {
    return sqlQuery;
  }

  /**
   * Sets the parameterized SQL query for the JDBC update operation.
   * This can be an insert, update, delete or a merge query.
   * Example: "update testTable set id = ? where name = ?"
   * @param sqlQuery the query statement
   */
  protected void setUpdateCommand(String sqlQuery)
  {
    this.sqlQuery = sqlQuery;
  }
}
