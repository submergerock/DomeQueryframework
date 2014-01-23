package org.cstor.cproc.cloudComputingFramework;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;

/** An client-datanode protocol for block recovery
 */
public interface JobProtocol {
  //public static final Log LOG = LogFactory.getLog(JobProtocol.class);
  
  /**
   * 3: add keepLength parameter.
   */
  public static final long versionID = 3L;

  public void setConfiguration(Configuration conf);
  
  public void setNameNode(ClientProtocol namenode);
  
  public void setDataNode(ClientDatanodeProtocol datanode);
  
  public void setCProcFrameworkProtocol(CProcFrameworkProtocol CProcFrameworkNode);
  
  public void setJob(Job j);
  
  public Configuration getConfiguration();  
  
  public Job getJob();
  
  public HashMap<String,StringBuffer> handle();
  
  public void stop();  
  
}

