package org.cstor.cproc.cloudComputingFramework.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.job.tools.getHostIP;
import org.apache.hadoop.net.NetUtils;
import org.cstor.cproc.cloudComputingFramework.CProcFrameworkProtocol;
import org.cstor.cproc.cloudComputingFramework.Job;
import org.cstor.cproc.cloudComputingFramework.JobEntity.JobEntityType;

public class DoSEARCH4 implements org.cstor.cproc.cloudComputingFramework.JobProtocol{
	public static final Log LOG = LogFactory.getLog(DoSEARCH4.class.getName());
	public  Configuration confg = null; 
	public  ClientProtocol nameNode = null;
	public  ClientDatanodeProtocol dataNode = null;
	public  Job job = null;	
	
	public  static int count  = 0;	
	
	public DoSEARCH4()  {  
		
	  }
	public DoSEARCH4(DataNode dataNode,Job e)  {
    this.job = e;
    this.dataNode = dataNode;    
  }
	
	public HashMap<String,StringBuffer> handle(){
		count++;
		

		LOG.info("********************************************************************************");
    	LOG.info("*                                                                              *");
    	LOG.info("*                         DoSEARCH(test) is running!!!           count:"+ count );
    	LOG.info("*                                                                              *");
    	LOG.info("********************************************************************************");

    	
	
		LOG.info("this job come from " + job.getConf().get("hdfs.job.from.ip"));
	
		LOG.info("hdfs.job.param.system.param : "
				+ job.getConf().get("hdfs.job.param.system.param") + "-->" + getHostIP.getLocalIP());

		
		CProcFrameworkProtocol frameworkNode = null;
		InetSocketAddress frameworkNodeAddr = NetUtils.createSocketAddr(this.job.getConf().get("hdfs.job.NN.ip").split(":")[0], 8888);
		Configuration conf = new Configuration();
		
		try {
			frameworkNode = (CProcFrameworkProtocol)RPC.getProxy(CProcFrameworkProtocol.class,
					CProcFrameworkProtocol.versionID, frameworkNodeAddr, conf,
			        NetUtils.getSocketFactory(conf, CProcFrameworkProtocol.class));
			long sleep = (long)(Math.random()*10);
			while(true){
				Thread.sleep(1000);
				LOG.info("-------sleeping:" + sleep + "---------");
				if(sleep == 0){
					break;
				}
				sleep --;
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
		frameworkNode.changeJobEntityType(this.job.getConf().get("hdfs.job.jobid"), JobEntityType.SUCCESS);
		frameworkNode.printJobEntityType(this.job.getJobId().toString());
	
		LOG.info("++++++++++++++++++++++++++++++DoSEARCH4++++++++++++++++++++++++++++++count:" + count);
		return null;
		
	}


    @Override
	public Configuration getConfiguration() {
		return this.confg;
	}
    
	public ClientDatanodeProtocol getDataNode() {
		return this.dataNode;
	}

	@Override
	public Job getJob() {
		return this.job;
	}

	public ClientProtocol getNameNode() {
		return this.nameNode;
	}

	@Override
	public void setConfiguration(Configuration conf) {
		this.confg = conf;		
	}

	@Override
	public void setJob(Job j) {
		this.job = j;
	}

	@Override
	public void setDataNode(ClientDatanodeProtocol dataNode) {
		this.dataNode = dataNode;		
	}

	@Override
	public void setNameNode(ClientProtocol nameNode) {
		this.nameNode = nameNode;		
	}
	public void stop() {
		LOG.info("-----------DoSEARCH stop!!------------");
	}
	@Override
	public void setCProcFrameworkProtocol(CProcFrameworkProtocol arg0) {
		// TODO Auto-generated method stub
		
	}
	
	
	
}
