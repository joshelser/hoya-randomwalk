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
package org.apache.hadoop.hoya.randomwalk.accumulo;

import java.util.List;
import java.util.Properties;

import org.apache.accumulo.randomwalk.State;
import org.apache.accumulo.randomwalk.error.InMemoryErrorReporter;
import org.apache.hadoop.hoya.providers.accumulo.AccumuloClientProvider;
import org.apache.hadoop.hoya.providers.accumulo.AccumuloConfigFileOptions;
import org.apache.hadoop.hoya.randomwalk.HoyaConfig;
import org.apache.hadoop.hoya.randomwalk.accumulo.CreateAccumuloCluster;
import org.apache.hadoop.hoya.yarn.Arguments;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * 
 */
public class CreateAccumuloClusterTest {

  private CreateAccumuloCluster create;

  @Before
  public void setup() {
    create = new CreateAccumuloCluster();
  }

  @Test
  public void testCommandGeneration() throws Exception {
    Properties props = new Properties();

    props.put(HoyaConfig.COMMAND, "/usr/bin/hoya");
    props.put(HoyaConfig.CLUSTER_NAME, "accumulo");
    props.put(HoyaConfig.MANAGER, "localhost:1234");
    props.put(HoyaConfig.FILESYSTEM, "localhost:8020");
    props.put(HoyaConfig.ZOOKEEPERS, "localhost");
    props.put(HoyaConfig.ZOOKEEPER_PATH, "/usr/lib/zookeeper");
    props.put(HoyaConfig.HADOOP_HOME, "/usr/lib/hadoop");
    props.put(HoyaConfig.ACCUMULO_IMAGE, "/accumulo.tar.gz");
    props.put(HoyaConfig.ACCUMULO_CONF, "/accumulo-conf");
    props.put(HoyaConfig.ACCUMULO_PASSWORD, "password");

    State state = new State(props, new InMemoryErrorReporter());

    List<String> actualCommand = create.getCommand(state);

    List<String> expectedCommand = Lists.newArrayList("/usr/bin/hoya", "create", "accumulo", "--manager localhost:1234", "--filesystem localhost:8020",
        Arguments.ARG_PROVIDER, "accumulo", Arguments.ARG_ROLE, "master", "1", Arguments.ARG_ROLE, "tserver", "1", Arguments.ARG_ROLE, "gc", "1",
        Arguments.ARG_ROLE, "monitor", "1", Arguments.ARG_ROLE, "tracer", "1", Arguments.ARG_ZKHOSTS + " localhost", Arguments.ARG_APP_ZKPATH
            + " /usr/lib/zookeeper", Arguments.ARG_IMAGE + " /accumulo.tar.gz", Arguments.ARG_CONFDIR + " /accumulo-conf", Arguments.ARG_OPTION,
        AccumuloClientProvider.OPTION_ZK_HOME, "/usr/lib/zookeeper", Arguments.ARG_OPTION, AccumuloClientProvider.OPTION_HADOOP_HOME, "/usr/lib/hadoop",
        Arguments.ARG_OPTION, AccumuloClientProvider.OPTION_ACCUMULO_PASSWORD, "password");

    Assert.assertEquals(expectedCommand, actualCommand);
  }

  @Test
  public void testCommandGenerationWithPort() throws Exception {
    Properties props = new Properties();

    props.put(HoyaConfig.COMMAND, "/usr/bin/hoya");
    props.put(HoyaConfig.CLUSTER_NAME, "accumulo");
    props.put(HoyaConfig.MANAGER, "localhost:1234");
    props.put(HoyaConfig.FILESYSTEM, "localhost:8020");
    props.put(HoyaConfig.ZOOKEEPERS, "localhost");
    props.put(HoyaConfig.ZOOKEEPER_PATH, "/usr/lib/zookeeper");
    props.put(HoyaConfig.HADOOP_HOME, "/usr/lib/hadoop");
    props.put(HoyaConfig.ACCUMULO_IMAGE, "/accumulo.tar.gz");
    props.put(HoyaConfig.ACCUMULO_CONF, "/accumulo-conf");
    props.put(HoyaConfig.ACCUMULO_PASSWORD, "password");
    props.put(HoyaConfig.ACCUMULO_MONITOR_PORT, "12345");

    State state = new State(props, new InMemoryErrorReporter());

    List<String> actualCommand = create.getCommand(state);

    List<String> expectedCommand = Lists.newArrayList("/usr/bin/hoya", "create", "accumulo", "--manager localhost:1234", "--filesystem localhost:8020",
        Arguments.ARG_PROVIDER, "accumulo", Arguments.ARG_ROLE, "master", "1", Arguments.ARG_ROLE, "tserver", "1", Arguments.ARG_ROLE, "gc", "1",
        Arguments.ARG_ROLE, "monitor", "1", Arguments.ARG_ROLE, "tracer", "1", Arguments.ARG_ZKHOSTS + " localhost", Arguments.ARG_APP_ZKPATH
            + " /usr/lib/zookeeper", Arguments.ARG_IMAGE + " /accumulo.tar.gz", Arguments.ARG_CONFDIR + " /accumulo-conf", Arguments.ARG_OPTION,
        AccumuloClientProvider.OPTION_ZK_HOME, "/usr/lib/zookeeper", Arguments.ARG_OPTION, AccumuloClientProvider.OPTION_HADOOP_HOME, "/usr/lib/hadoop",
        Arguments.ARG_OPTION, AccumuloClientProvider.OPTION_ACCUMULO_PASSWORD, "password", Arguments.ARG_OPTION, "site."
            + AccumuloConfigFileOptions.MONITOR_PORT_CLIENT, "12345");

    Assert.assertEquals(expectedCommand, actualCommand);
  }

}
