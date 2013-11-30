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
package org.apache.hadoop.hoya.randomwalk;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.accumulo.randomwalk.State;
import org.apache.accumulo.randomwalk.error.InMemoryErrorReporter;
import org.apache.hadoop.hoya.randomwalk.conf.HoyaConfig;
import org.apache.hadoop.hoya.yarn.Arguments;
import org.apache.hadoop.hoya.yarn.HoyaActions;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterables;

/**
 * 
 */
public class FlexClusterTest {

  @Test
  public void testSuccessfulCommand() {
    Properties props = new Properties();

    props.put(HoyaConfig.COMMAND, "/usr/bin/hoya");
    props.put(HoyaConfig.MANAGER, "localhost:1234");
    props.put(HoyaConfig.FILESYSTEM, "hdfs://localhost:8020");
    props.put(HoyaConfig.CLUSTER_NAME, "cluster");

    FlexCluster flex = new FlexCluster();

    State state = new State(props, new InMemoryErrorReporter());

    List<String> command = flex.getCommand(state);

    List<String> expected = Arrays.asList("/usr/bin/hoya", HoyaActions.ACTION_FLEX, "cluster", Arguments.ARG_MANAGER + " localhost:1234",
        Arguments.ARG_FILESYSTEM_LONG + " hdfs://localhost:8020");
    
    Assert.assertEquals(expected, command);
    
    Assert.assertEquals(0, Iterables.size(state.getErrorReporter().report()));
  }

  @Test
  public void testMissingClustername() {
    Properties props = new Properties();

    props.put(HoyaConfig.COMMAND, "/usr/bin/hoya");
    props.put(HoyaConfig.MANAGER, "localhost:1234");
    props.put(HoyaConfig.FILESYSTEM, "hdfs://localhost:8020");

    FlexCluster flex = new FlexCluster();

    State state = new State(props, new InMemoryErrorReporter());

    try {
      flex.getCommand(state);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // pass
    }

    Assert.assertEquals(1, Iterables.size(state.getErrorReporter().report()));
  }
}
