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
package org.apache.hadoop.hoya.randomwalk.conf;

import java.util.Properties;

import org.apache.accumulo.randomwalk.Node;
import org.apache.accumulo.randomwalk.State;
import org.apache.accumulo.randomwalk.error.InMemoryErrorReporter;
import org.apache.hadoop.hoya.randomwalk.conf.HoyaConfig;
import org.apache.hadoop.hoya.randomwalk.conf.RandomwalkHoyaConfiguration;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class RandomwalkHoyaConfigurationTest {
  
  private static class TestNode extends Node {
    @Override
    public void visit(State s, Properties p) {} 
  }

  @Test
  public void testGetPresentKey() {
    Properties props = new Properties();
    props.put("foo", "bar");
    
    State state = new State(props, new InMemoryErrorReporter());
    RandomwalkHoyaConfiguration builder = new RandomwalkHoyaConfiguration(TestNode.class, state);
    
    Assert.assertEquals("bar", builder.getStringOrThrow("foo"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetAbsentKey() {
    Properties props = new Properties();
    
    State state = new State(props, new InMemoryErrorReporter());
    RandomwalkHoyaConfiguration builder = new RandomwalkHoyaConfiguration(TestNode.class, state);
    
    Assert.assertEquals("bar", builder.getStringOrThrow("foo"));
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testNonStringValue() {
    Properties props = new Properties();
    props.put("foo", 1);
    
    State state = new State(props, new InMemoryErrorReporter());
    RandomwalkHoyaConfiguration builder = new RandomwalkHoyaConfiguration(TestNode.class, state);
    
    // It will only pull Strings from the Properties
    builder.getStringOrThrow("foo");
  }
  
  @Test
  public void testOptionalMonitor() {
    Properties props = new Properties();

    props.put(HoyaConfig.ACCUMULO_MONITOR_PORT, "12345");

    State state = new State(props, new InMemoryErrorReporter());

    RandomwalkHoyaConfiguration builder = new RandomwalkHoyaConfiguration(TestNode.class, state);

    Assert.assertTrue(builder.isMonitorSet());
    Assert.assertEquals("12345", builder.getMonitorPort());

    props = new Properties();
    state = new State(props, new InMemoryErrorReporter());
    builder = new RandomwalkHoyaConfiguration(TestNode.class, state);

    Assert.assertFalse(builder.isMonitorSet());
  }

}
