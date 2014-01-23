package org.cstor.cproc.cloudComputingFramework.test;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.job.tools.getHostIP;
import org.cstor.cproc.cloudComputingFramework.CProcFrameworkProtocol;
import org.cstor.cproc.cloudComputingFramework.Job;

public class DoNNDIR4 implements org.cstor.cproc.cloudComputingFramework.JobProtocol{
	public static final Log LOG = LogFactory.getLog(DoNNDIR4.class.getName());
	public  Configuration confg = null; 
	public  ClientProtocol nameNode = null;
	public  ClientDatanodeProtocol dataNode = null;
	public  Job job = null;
	
	public DoNNDIR4() { 
  }
	
	public DoNNDIR4(NameNode nameNode,Job e,Configuration confg) {
    this.job = e;
    this.nameNode = nameNode;  
    this.confg = confg;    
  }
	
	public HashMap<String,StringBuffer> handle(){

		LOG.info("********************************************************************************");
    	LOG.info("*                                                                              *");
    	LOG.info("*                         DoNNDIR(test) is running!!!                          *");
    	LOG.info("*                                                                              *");
    	LOG.info("********************************************************************************");

		LOG.info("MyJob:" + "this job come from " + job.getConf().get("hdfs.job.from.ip") + 
				"-->" + getHostIP.getLocalIP());
		LOG.info("MyJob:" + "hdfs.job.param.sessionId : " + job.getConf().get("hdfs.job.param.sessionId"));
		job.getConf().set("hdfs.job.class","org.cstor.cproc.cloudComputingFramework.test.DoINDEX4");
		
	
		if(this.nameNode == null)
			return null;

		HashMap<String,StringBuffer> DNIPtoIndexFiles = new HashMap<String,StringBuffer>();
	
		String[] DNIPs = job.getConf().getStrings("hdfs.job.DNIPs");
		for(String DNIp : DNIPs){
			DNIPtoIndexFiles.put(DNIp,(new StringBuffer(job.getConf().get("hdfs.job.from.ip") + 
					"-->" + getHostIP.getLocalIP())));
		}
		job.getConf().set("hdfs.job.from.ip",getHostIP.getLocalIP());
		for(String DNIp : DNIPs){
			LOG.info("DNIPtoFileandOffsets.get("+ DNIp +"):" +DNIPtoIndexFiles.get(DNIp));
		}
		
		LOG.info("+++++++++++++++++++++++++DoNNDIR4++++++++++++++++++++++++++");
		return DNIPtoIndexFiles;
		//return null;
	}
	
	@Override
	public Configuration getConfiguration() {
		return this.confg;
	}

	public ClientDatanodeProtocol getDataNode() {
		return this.dataNode;
	}

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

	public void setJob(Job j) {
		this.job = j;
	}
	
	@Override
	public void stop() {
		LOG.info("-----------DoNNDIR stop!!------------");
	}

	@Override
	public void setDataNode(ClientDatanodeProtocol dataNode) {
		this.dataNode = dataNode;		
	}

	@Override
	public void setNameNode(ClientProtocol nameNode) {
		this.nameNode = nameNode;		
	}

	@Override
	public void setCProcFrameworkProtocol(CProcFrameworkProtocol arg0) {
		// TODO Auto-generated method stub
		
	}

}
