package org.apache.hadoop.hoya.randomwalk.accumulo;

import java.util.List;
import java.util.Properties;

import org.apache.accumulo.randomwalk.State;
import org.apache.accumulo.randomwalk.Test;
import org.apache.hadoop.hoya.providers.accumulo.AccumuloClientProvider;
import org.apache.hadoop.hoya.providers.accumulo.AccumuloConfigFileOptions;
import org.apache.hadoop.hoya.providers.accumulo.AccumuloKeys;
import org.apache.hadoop.hoya.randomwalk.Constants;
import org.apache.hadoop.hoya.randomwalk.CreateCluster;
import org.apache.hadoop.hoya.randomwalk.HoyaProcessBuilder;
import org.apache.hadoop.hoya.yarn.Arguments;
import org.apache.hadoop.hoya.yarn.HoyaActions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Wrapper around creating an Accumulo cluster with Hoya
 */
public class CreateAccumuloCluster extends Test implements CreateCluster {
  private static final Logger log = LoggerFactory.getLogger(CreateAccumuloCluster.class);

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

  public List<String> getCommand(State state) {
    HoyaProcessBuilder hoyaBuilder = new HoyaProcessBuilder(state);

    List<String> command = Lists.newArrayList(hoyaBuilder.getHoyaCommand());

    command.add(HoyaActions.ACTION_CREATE);
    command.add(hoyaBuilder.getClusterName());

    command.add(hoyaBuilder.getManagerWithOption());

    command.add(hoyaBuilder.getFilesystemWithOption());

    command.add(Arguments.ARG_PROVIDER);
    command.add(Constants.ACCUMULO);

    command.add(Arguments.ARG_ROLE);
    command.add(AccumuloKeys.ROLE_MASTER);
    command.add(Integer.toString(1));

    command.add(Arguments.ARG_ROLE);
    command.add(AccumuloKeys.ROLE_TABLET);
    command.add(Integer.toString(1));

    command.add(Arguments.ARG_ROLE);
    command.add(AccumuloKeys.ROLE_GARBAGE_COLLECTOR);
    command.add(Integer.toString(1));

    command.add(Arguments.ARG_ROLE);
    command.add(AccumuloKeys.ROLE_MONITOR);
    command.add(Integer.toString(1));

    command.add(Arguments.ARG_ROLE);
    command.add(AccumuloKeys.ROLE_TRACER);
    command.add(Integer.toString(1));

    command.add(hoyaBuilder.getZooKeepersWithOption());

    command.add(hoyaBuilder.getZooKeeperPathWithOption());

    command.add(hoyaBuilder.getAccumuloImageWithOption());

    command.add(hoyaBuilder.getAccumuloAppConfWithOption());

    command.add(Arguments.ARG_OPTION);
    command.add(AccumuloClientProvider.OPTION_ZK_HOME);
    command.add(hoyaBuilder.getZooKeeperPath());

    command.add(Arguments.ARG_OPTION);
    command.add(AccumuloClientProvider.OPTION_HADOOP_HOME);
    command.add(hoyaBuilder.getHadoopHome());
    
    command.add(Arguments.ARG_OPTION);
    command.add(AccumuloClientProvider.OPTION_ACCUMULO_PASSWORD);
    command.add(hoyaBuilder.getAccumuloPassword());

    if (hoyaBuilder.isMonitorSet()) {
      command.add(Arguments.ARG_OPTION);
      command.add("site." + AccumuloConfigFileOptions.MONITOR_PORT_CLIENT);
      command.add(hoyaBuilder.getMonitorPort());
    }

    return command;
  }
}
