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

import java.util.List;
import java.util.Properties;

import org.apache.accumulo.randomwalk.State;
import org.apache.accumulo.randomwalk.Test;
import org.apache.hadoop.hoya.randomwalk.conf.RandomwalkHoyaConfiguration;
import org.apache.hadoop.hoya.yarn.HoyaActions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * 
 */
public class DestroyCluster extends Test {
  private static final Logger log = LoggerFactory.getLogger(DestroyCluster.class);

  @Override
  public void visit(State state, Properties props) throws Exception {
    List<String> command = getCommand(state);

    Process proc = new ProcessBuilder(command).inheritIO().start();

    int returnCode;
    try {
      returnCode = proc.waitFor();
    } catch (InterruptedException e) {
      log.warn("InterruptedException while waiting for list command to complete", e);
      // If we get interrupted, try to kill the Process
      proc.destroy();
      return;
    }

    if (0 != returnCode) {
      log.error("Got non-zero exit code from list command: {}", command);
    }
  }

  protected List<String> getCommand(State state) {
    RandomwalkHoyaConfiguration conf = new RandomwalkHoyaConfiguration(DestroyCluster.class, state);

    List<String> command = Lists.newArrayList(conf.getHoyaCommand());

    command.add(HoyaActions.ACTION_DESTROY);
    command.add(conf.getClusterName());

    command.add(conf.getManagerWithOption());
    command.add(conf.getFilesystemWithOption());

    return command;
  }
}
