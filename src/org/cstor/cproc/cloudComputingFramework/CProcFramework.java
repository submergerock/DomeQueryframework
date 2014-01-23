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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.job.tools.GCThread;
import org.apache.hadoop.job.tools.getHostIPcProc;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.cstor.cproc.cloudComputingFramework.JobEntity.JobEntity;
import org.cstor.cproc.cloudComputingFramework.JobEntity.JobEntityStore;
import org.cstor.cproc.cloudComputingFramework.JobEntity.JobEntityType;

/**
 * @author wanglei
 * 
 * @version 0.1
 * 
 */

public class CProcFramework  implements  CProcFrameworkProtocol{

  public static final Log LOG = LogFactory.getLog(CProcFramework.class.getName());
  public static  String network = null; 
  public static  String namenodeAddress = null; 
  public static  String port = null; 
  public static  String ipAddr = null;
  
  private CProcConfiguration cprocConf; // configuration of local standby namenode
  private RpcConfiguration rpcConf; // configuration of local standby namenode
  
  private ClientProtocol nameNode = null;
  private ClientDatanodeProtocol dataNode = null;
  
  Thread jobControlThread;
  private JobControl jobControl = null;
  
  /** RPC server */
  private Server server;  

  public  boolean isNN ;
  
  public static JobEntityStore jobEntityStore = null;
  private InetSocketAddress nameNodeAddr = null;
  static {
	    Configuration.addDefaultResource("cproc-site.xml");
	  }
  
  
  public CProcFramework(ClientProtocol nameNode, ClientDatanodeProtocol dataNode,
		  CProcConfiguration cprocConf,RpcConfiguration rpcConf,boolean isNN,InetSocketAddress nameNodeAddr) {
	    this.nameNode = nameNode;
	    this.dataNode = dataNode;
	    this.cprocConf = cprocConf; 
	    this.rpcConf = rpcConf;
	    this.isNN = isNN;	    
	    this.nameNodeAddr = nameNodeAddr;
	    try {
			initialize();
		} catch (IOException e) {
			e.printStackTrace();
		}
  }
  
  /**
   * Initialize name-node.
   * 
   * @param conf the configuration
   */
  private void initialize() throws IOException {
	String port = this.port == null ? this.rpcConf.get("cproc.node.port","8888") : this.port;
	LOG.info("port : " + port);
	InetSocketAddress socAddr = NetUtils.createSocketAddr("0.0.0.0:" + port);
	//cprocframework里面有 2个主类 JobEntityStore,JobControl
    // create rpc server 
    this.server = RPC.getServer(this, socAddr.getHostName(), socAddr.getPort(),
    		this.rpcConf.getInt("cproc.node.handle",256), false, this.rpcConf);

    LOG.info("CProcFramework node up at: " + this.server.getListenerAddress());
    //JObEntityStore的作用.JobEntityStore属于master的启动类，参数什么意思
    if(this.isNN == true){
    	this.jobEntityStore = new JobEntityStore(this.rpcConf.getInt("cproc.jobentity.depthOfPrintJobEntityType",3));
    }
    this.server.start();  //start RPC server   

    //JobControl的作用
    this.jobControl = new JobControl(this.nameNode,this.dataNode,this.cprocConf,this.rpcConf,this.isNN,this.network,this.nameNodeAddr);
    this.jobControlThread = new Thread(this.jobControl);
    this.jobControlThread.start();
     
  }

  public static CProcFramework createCProcFramework(ClientProtocol nameNode,ClientDatanodeProtocol dataNode
		  ,CProcConfiguration cprocConf,RpcConfiguration rpcConf,boolean isNN,InetSocketAddress nameNodeAddr){
	  
	  
    return new CProcFramework(nameNode,dataNode,cprocConf,rpcConf,isNN,nameNodeAddr);
  }
  
  public static void main(String argv[]) throws Exception {
	  
	   LOG.info("CProcFramework  version for hadoop-0.20.2-patch : 32");	   
	   
	   //调用GCThread线程，定时GC
	   Thread th = new Thread(new GCThread());
		th.start();
	   
	   //LOG.info("CProcFramework [NameNode IP:Port]");
	   StringUtils.startupShutdownMessage(CProcFramework.class, argv, LOG);
	   //hdfs-site.xml,core-site.xml
	   CProcConfiguration cprocConf = CProcConfiguration.getCProcConfiguration();
	   RpcConfiguration rpcConf = RpcConfiguration.getRpcConfiguration(); 
	   setParamToRpcConf(cprocConf,rpcConf);
	   
       CProcFramework cpf = null;
       ClientProtocol nameNode = null;
       ClientDatanodeProtocol dataNode = null;	
       InetSocketAddress dataNodeAddr = null;
  	   InetSocketAddress nameNodeAddr = null;	
	    try {  
	  	
	  	  boolean isNN = true;
	  	  
	  	  //LOG.info("conf.get(\"fs.default.name\") == " + conf.get("cProc.name.") );
	      //wzt ??? 命令行参数是什么样子的 net0 192.168.139.21 9000
	  	  if(argv.length >= 3){
	  		  network = argv[0];
	  		  namenodeAddress = argv[1];
	  		  port = argv[2];
	  	  } else {
	  		  
	  			LOG.info("CProcFramework [network adapter] [namenode Address]");
		  		System.exit(0);	  		  
	  		
	  	  }
	  	 LOG.info("network = " + network);

	  		try{
	    		  nameNodeAddr = NetUtils.createSocketAddr(namenodeAddress);
	    	  } catch (IllegalArgumentException e){
	    		  //nameNodeAddr = NetUtils.createSocketAddr(argv[0]);
	    	  }
	  	  
	      LOG.info("NameNodeAddr = " + nameNodeAddr);	      
	      
	      //-----------------------get all DN and get conf of NN----------------------------------
	      
	  	  ArrayList<String> DNIP = new ArrayList<String>();

	  	 // Configuration confInFunc = new Configuration();
	  	  
		  nameNode = (ClientProtocol) RPC.getProxy(ClientProtocol.class,
				ClientProtocol.versionID, nameNodeAddr, rpcConf, NetUtils
						.getSocketFactory(rpcConf, ClientProtocol.class));
		
		  DatanodeDescriptor[] dds = nameNode.getAllDatanode();
		  
		 // LOG.info("==========");
		  for(DatanodeDescriptor dd: dds){
			  LOG.info(dd.getHost());
			  //LOG.info(dd.getHostName());
			  //LOG.info(dd.getDatanodeReport());
			  DNIP.add(dd.getHost());
		  }
		  //LOG.info("==========");
	      //conf = nameNode.getConfiguration();
		  setParamToCprocConf(nameNode.getConfiguration(),rpcConf);
		  
	  	  LOG.info("getHostIP.getLocalIP() = " + getHostIPcProc.getLocalIP(network));
	  	  
	  	//-----------------------if this node is a DN get conf----------------------------------
	  	  
	      if(DNIP.contains(getHostIPcProc.getLocalIP(network))){
	    	dataNodeAddr = NetUtils.createSocketAddr("127.0.0.1:" + rpcConf.get("dfs.datanode.ipc.address").split(":")[1]);

    		dataNode = (ClientDatanodeProtocol) RPC.getProxy(ClientDatanodeProtocol.class,
    				  ClientDatanodeProtocol.versionID, dataNodeAddr, rpcConf, NetUtils
	  							.getSocketFactory(rpcConf, ClientDatanodeProtocol.class));

	  		//conf = dataNode.getConfiguration();
	  		LOG.info("This is DataNode!!");
	  		isNN = false;
	      } else {
	    	    LOG.info("This is NameNode!!");
	    	    isNN = true;
	      }	 
	      
	      cpf = createCProcFramework(nameNode,dataNode,cprocConf,rpcConf,isNN,nameNodeAddr) ;
	      
	      cpf.waitForStop();
	      
	    } catch (Throwable e) {
	      LOG.error(StringUtils.stringifyException(e));
	      System.exit(-1);
	    } finally{
	    	//20131210修改RPC关闭
//	    	if(nameNode != null){
//	    		RPC.stopProxy(nameNode);
//	    	}
//	    	if(dataNode != null){
//	    		RPC.stopProxy(dataNode);
//	    	}
	    }
	  }
   
   /**
    * Wait for the CProcNode to exit. .
    */
  private void waitForStop() {
	   if(this.server != null){
		   try {
			      this.server.join();
			    } catch (InterruptedException ie) {
			    }
	   }	   
	   if(this.jobControlThread != null){
		   try {
			      this.jobControlThread.join();
			    } catch (InterruptedException ie) {
			    }
	   }	   
   }

  public  void submitJob(Job e){
	   this.jobControl.putJob(e);
   }
      
@Override
  public long getProtocolVersion(String protocol, long clientVersion)
		throws IOException {
	if (protocol.equals(CProcFrameworkProtocol.class.getName())) {
	      return CProcFrameworkProtocol.versionID; 
	    } else {
	      throw new IOException("Unknown protocol to CProcFramework node: " + protocol);
	    }
    }

@Override
public void changeJobEntityType(String JobId, JobEntityType jobEntityType) {
	this.jobEntityStore.changeJobEntityType(JobId, jobEntityType);	
}

@Override
public String getJobEntityType(String JobId) {
	
	String JobIdTmp = JobId;
	
	if(JobId.indexOf(",") != -1)		
		JobIdTmp = JobId.substring(0 , JobId.indexOf(","));	
	
	JobEntityType jobEntityType = this.jobEntityStore.getJobEntityType(JobIdTmp);
	
	if(jobEntityType == null){
		return "JobId is null!!";
	} else {
		if(jobEntityType == JobEntityType.RUNNING){
			return "RUNNING";
		} else if(jobEntityType == JobEntityType.SUCCESS){
			return "SUCCESS";
		} else {
			return "error status!!";
		}
	}		
}
@Override
public JobEntityType getJobEntityTypeByJobEntityType(String JobId) {
	
	String JobIdTmp = JobId;
	
	if(JobId.indexOf(",") != -1)		
		JobIdTmp = JobId.substring(0 , JobId.indexOf(","));	
	
	JobEntityType jobEntityType = this.jobEntityStore.getJobEntityType(JobIdTmp);
	
	return jobEntityType;
	
}

@Override
public synchronized void printJobEntityType(String JobId,String useTime) {
	this.jobEntityStore.printJobEntityType(JobId,useTime);
	
}

@Override
public synchronized void printJobEntityType(String JobId) {
	this.jobEntityStore.printJobEntityType(JobId,"");
	
}

public void stopJob(String jobid){
	this.jobControl.stopJob(jobid);
}

@Override
public void submitJob(Job job, Text param) {
	synchronized (job.getConf()) {
		job.getConf().set("hdfs.job.param.system.param", param.toString());
	}
	this.submitJob(job);
	
}

private static void setParamToRpcConf(CProcConfiguration cprocConf,RpcConfiguration rpcConf){	
	   
	if(rpcConf.get("dfs.datanode.ipc.address") != null)
	   cprocConf.set("dfs.datanode.ipc.address", rpcConf.get("dfs.datanode.ipc.address"));
	if(rpcConf.get("cproc.node.port") != null)
	   cprocConf.set("cproc.node.port", rpcConf.get("cproc.node.port"));
	if(rpcConf.get("cproc.node.handle") != null)
	   cprocConf.set("cproc.node.handle", rpcConf.get("cproc.node.handle"));
	if(rpcConf.get("cproc.jobentity.depthOfPrintJobEntityType") != null)
	   cprocConf.set("cproc.jobentity.depthOfPrintJobEntityType", rpcConf.get("cproc.jobentity.depthOfPrintJobEntityType"));
	if(rpcConf.get("cproc.threadpool.handler.count") != null)
	   cprocConf.set("cproc.threadpool.handler.count", rpcConf.get("cproc.threadpool.handler.count"));
	if(rpcConf.get("fs.default.name") != null)
		   cprocConf.set("fs.default.name", rpcConf.get("fs.default.name"));
	if(rpcConf.get("fs.hdfs.impl") != null)
		   cprocConf.set("fs.hdfs.impl", rpcConf.get("fs.hdfs.impl"));
				   
}

private static void setParamToCprocConf(Configuration conf,RpcConfiguration rpcConf){	
	   
	  if(conf.get("dfs.datanode.ipc.address") != null){
		rpcConf.set("dfs.datanode.ipc.address", conf.get("dfs.datanode.ipc.address"));
	  }
				   
}

@Override
public JobEntity getJobEntity(String JobId) {
	// TODO Auto-generated method stub
	LOG.info("--------getJobEntity---------");
	JobEntity jobEntity = this.jobEntityStore.getJobEntity(JobId);
	
	LOG.info("++++++++++getJobEntity+++++++++");
	return jobEntity;
}



//private synchronized void stopJobInternal(String jobid){
//	ConcurrentHashMap<String,Thread> JobidToThread = this.jobControl.getJobidToThread();
//	for(String jid : JobidToThread.keySet()){
//		if(jid.split(",")[0].equals(jobid)){
//			JobidToThread.get(jid).destroy();
//		}
//	}
//}

}
