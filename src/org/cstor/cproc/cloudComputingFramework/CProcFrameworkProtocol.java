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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.cstor.cproc.cloudComputingFramework.JobEntity.JobEntity;
import org.cstor.cproc.cloudComputingFramework.JobEntity.JobEntityType;

/** An client-datanode protocol for block recovery
 */
public interface CProcFrameworkProtocol extends VersionedProtocol{
  //public static final Log LOG = LogFactory.getLog(JobProtocol.class);
  
	public static final long versionID = 88L;

	
  /**
   * A job is submited from client to framework
   * 
   * @param job a job instance
   **/
  public void submitJob(Job job);
  
  public void submitJob(Job job,Text param);
  
  /**
   * A job is submited from client to framework
   * 
   * @param Job a job instance
   * @param Job a job instance
   **/
  public void changeJobEntityType(String JobId,JobEntityType jobEntityType);
  
  public String getJobEntityType(String JobId);
  
  public JobEntity getJobEntity(String JobId);
  
  public JobEntityType getJobEntityTypeByJobEntityType(String JobId);
  
  public void printJobEntityType(String JobId,String useTime);
  
  public void printJobEntityType(String JobId);
  
  public void stopJob(String jobid);
  //public ListWritable testWritable(ListWritable JobId);
}