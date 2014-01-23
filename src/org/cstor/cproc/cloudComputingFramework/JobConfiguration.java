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

package org.cstor.cproc.cloudComputingFramework;


import java.io.File;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;


/**
 * @author WangLei 
 * @version 0.4  
 * Filename :  DataMode.java
 * Copyright : Copyright 2012 Cstor Tech. Co. Ltd. All Rights Reserved.
 * Describe : 
 * Created time : 2012-5-12
 */

  public class JobConfiguration extends Configuration {
	  private static final Log LOG = LogFactory.getLog(JobConfiguration.class.getName());
	 
	  private static JobConfiguration jobConfiguration = null;	  
	  
	  public JobConfiguration() {
	    super(false);
	  }
	  
	public static JobConfiguration getJobConfiguration(){
		if(jobConfiguration == null){
			jobConfiguration = new JobConfiguration();
		}
		return jobConfiguration;
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
	}
  }
 
