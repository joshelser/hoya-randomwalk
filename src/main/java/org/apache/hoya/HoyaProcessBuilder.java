package org.apache.hoya;

import org.apache.accumulo.randomwalk.State;
import org.apache.hadoop.hoya.yarn.Arguments;

public class HoyaProcessBuilder {
  private State state;
  
  public HoyaProcessBuilder(State state) {
    this.state = state;
  }
  
  public String getHoyaCommand() {
    return state.getProperty(HoyaConfig.COMMAND);
  }
  
  public String getManagerWithOption() {
    return Arguments.ARG_MANAGER + Constants.SPACE + getManager();
  }
  
  public String getManager() {
    String manager = state.getProperty(HoyaConfig.MANAGER);
    
    if (null == manager) {
      throw new IllegalArgumentException(HoyaConfig.MANAGER + " was not provided in configuration.");
    }
    
    return manager;
  }
  
  public String getFilesystemWithOption() {
    return Arguments.ARG_FILESYSTEM_LONG + Constants.SPACE + getFilesystem();
  }
  
  public String getFilesystem() {
    String filesystem = state.getProperty(HoyaConfig.FILESYSTEM);
    
    if (null == filesystem) {
      throw new IllegalArgumentException(HoyaConfig.FILESYSTEM + " was not provided in configuration.");
    }
    
    return filesystem;
  }
}
