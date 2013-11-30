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

public class FreezeCluster extends Test{
  private static final Logger log = LoggerFactory.getLogger(FreezeCluster.class);

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
    RandomwalkHoyaConfiguration conf = new RandomwalkHoyaConfiguration(FreezeCluster.class, state);
    
    List<String> command = Lists.newArrayList(conf.getHoyaCommand());
    
    command.add(HoyaActions.ACTION_FREEZE);
    command.add(conf.getClusterName());
    
    command.add(conf.getManagerWithOption());
    command.add(conf.getFilesystemWithOption());
    
    return command;
  }
}
