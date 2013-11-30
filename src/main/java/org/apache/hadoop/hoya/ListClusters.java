package org.apache.hadoop.hoya;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.accumulo.randomwalk.State;
import org.apache.accumulo.randomwalk.Test;
import org.apache.accumulo.randomwalk.error.ErrorReport;
import org.apache.hadoop.hoya.yarn.HoyaActions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListClusters extends Test {
  private static final Logger log = LoggerFactory.getLogger(ListClusters.class);
  
  public ListClusters() { }
  
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
    HoyaProcessBuilder hoyaBuilder = new HoyaProcessBuilder(state);
    String hoyaCmd = hoyaBuilder.getHoyaCommand();
    
    if (null == hoyaCmd) {
      state.getErrorReporter().addError(new ErrorReport(ListClusters.class, HoyaConfig.COMMAND + " was not defined"));
      throw new IllegalArgumentException("Missing configuration value for " + HoyaConfig.COMMAND);
    }
    
    String action = HoyaActions.ACTION_LIST;
    
    String manager;
    try {
      manager = hoyaBuilder.getManagerWithOption();
    } catch (IllegalArgumentException e) {
      state.getErrorReporter().addError(new ErrorReport(ListClusters.class, HoyaConfig.MANAGER + " was not defined"));
      throw e;
    }
    
    String filesystem;
    try {
      filesystem = hoyaBuilder.getFilesystemWithOption();
    } catch (IllegalArgumentException e) {
      state.getErrorReporter().addError(new ErrorReport(ListClusters.class, HoyaConfig.FILESYSTEM + " was not defined"));
      throw e;
    }
    
    return Arrays.asList(hoyaCmd, action, manager, filesystem);
  }
}
