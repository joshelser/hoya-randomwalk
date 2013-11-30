package org.apache.hadoop.hoya;

import org.apache.accumulo.randomwalk.State;
import org.apache.hadoop.hoya.yarn.Arguments;

import com.google.common.base.Preconditions;

public class HoyaProcessBuilder {
  private State state;
  
  public HoyaProcessBuilder(State state) {
    this.state = state;
  }
  
  public String getHoyaCommand() {
    return getStringOrThrow(HoyaConfig.COMMAND);
  }
  
  public String getManagerWithOption() {
    return Arguments.ARG_MANAGER + Constants.SPACE + getManager();
  }
  
  public String getManager() {
    return getStringOrThrow(HoyaConfig.MANAGER);
  }
  
  public String getFilesystemWithOption() {
    return Arguments.ARG_FILESYSTEM_LONG + Constants.SPACE + getFilesystem();
  }
  
  public String getFilesystem() {
    return getStringOrThrow(HoyaConfig.FILESYSTEM);
  }
  
  public String getClusterName() {
    return getStringOrThrow(HoyaConfig.CLUSTER_NAME);
  }
  
  public String getZooKeepersWithOption() {
    return Arguments.ARG_ZKHOSTS + Constants.SPACE + getZooKeepers();
  }
  
  public String getZooKeepers() {
    return getStringOrThrow(HoyaConfig.ZOOKEEPERS);
  }
  
  public String getZooKeeperPathWithOption() {
    return Arguments.ARG_APP_ZKPATH + Constants.SPACE + getZooKeeperPath();
  }
  
  public String getZooKeeperPath() {
    return getStringOrThrow(HoyaConfig.ZOOKEEPER_PATH);
  }
  
  public String getAccumuloImageWithOption() {
    return Arguments.ARG_IMAGE + Constants.SPACE + getAccumuloImage();
  }
  
  public String getAccumuloImage() {
    return getStringOrThrow(HoyaConfig.ACCUMULO_IMAGE);
  }
  
  public String getAccumuloAppConfWithOption() {
    return Arguments.ARG_CONFDIR + Constants.SPACE + getAccumuloAppConf();
  }
  
  public String getAccumuloAppConf() {
    return getStringOrThrow(HoyaConfig.ACCUMULO_CONF);
  }
  
  public String getHadoopHome() {
    return getStringOrThrow(HoyaConfig.HADOOP_HOME);
  }
  
  public String getAccumuloPassword() {
    return getStringOrThrow(HoyaConfig.ACCUMULO_PASSWORD);
  }
  
  public boolean isMonitorSet() {
    return null != state.getProperty(HoyaConfig.ACCUMULO_MONITOR_PORT);
  }
  
  public String getMonitorPort() {
    return getStringOrThrow(HoyaConfig.ACCUMULO_MONITOR_PORT);
  }
  
  /**
   * Pull the provided from the {@link State}'s properties, throwing an error when a value is not present.
   * @param key
   * @return
   */
  protected String getStringOrThrow(String key) {
    Preconditions.checkNotNull(key);
    
    String value = state.getProperty(key);
    
    if (null == value) {
      throw new IllegalArgumentException(key + " was not provided in configuration.");
    }
    
    return value;
  }
}
