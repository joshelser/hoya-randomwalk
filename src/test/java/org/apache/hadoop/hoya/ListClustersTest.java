/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hoya;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.randomwalk.State;
import org.apache.accumulo.randomwalk.error.ErrorReport;
import org.apache.accumulo.randomwalk.error.InMemoryErrorReporter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ListClustersTest {

  private ListClusters list;

  @Before
  public void setup() {
    list = new ListClusters();
  }

  @Test
  public void testCommandGeneration() throws Exception {
    Properties props = new Properties();

    props.put(HoyaConfig.COMMAND, "/usr/bin/hoya");
    props.put(HoyaConfig.MANAGER, "localhost:1234");
    props.put(HoyaConfig.FILESYSTEM, "localhost:8020");

    State state = new State(props, new InMemoryErrorReporter());

    List<String> actualCommand = list.getCommand(state);

    List<String> expectedCommand = Lists.newArrayList("/usr/bin/hoya", "list", "--manager localhost:1234", "--filesystem localhost:8020");

    Assert.assertEquals(expectedCommand, actualCommand);
  }

  @Test
  public void testMissingCommand() throws Exception {
    Properties props = new Properties();

    props.put(HoyaConfig.MANAGER, "localhost:1234");
    props.put(HoyaConfig.FILESYSTEM, "localhost:8020");

    State state = new State(props, new InMemoryErrorReporter());

    try {
      list.getCommand(state);
      Assert.fail("getCommand should not have successfully completed");
    } catch (IllegalArgumentException e) {
      // Accept
    }
    
    ArrayList<Entry<ErrorReport,Long>> errors = Lists.newArrayList(state.getErrorReporter().report());
    
    Assert.assertEquals(1, errors.size());
  }

  @Test
  public void testMissingManager() throws Exception {
    Properties props = new Properties();

    props.put(HoyaConfig.COMMAND, "/usr/bin/hoya");
    props.put(HoyaConfig.FILESYSTEM, "localhost:8020");

    State state = new State(props, new InMemoryErrorReporter());

    try {
      list.getCommand(state);
      Assert.fail("getCommand should not have successfully completed");
    } catch (IllegalArgumentException e) {
      // Accept
    }
    
    ArrayList<Entry<ErrorReport,Long>> errors = Lists.newArrayList(state.getErrorReporter().report());
    
    Assert.assertEquals(1, errors.size());
  }


  @Test
  public void testMissingFilesystem() throws Exception {
    Properties props = new Properties();

    props.put(HoyaConfig.COMMAND, "/usr/bin/hoya");
    props.put(HoyaConfig.MANAGER, "localhost:1234");

    State state = new State(props, new InMemoryErrorReporter());

    try {
      list.getCommand(state);
      Assert.fail("getCommand should not have successfully completed");
    } catch (IllegalArgumentException e) {
      // Accept
    }
    
    ArrayList<Entry<ErrorReport,Long>> errors = Lists.newArrayList(state.getErrorReporter().report());
    
    Assert.assertEquals(1, errors.size());
  }

}
