package org.cstor.cproc.cloudComputingFramework.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.job.tools.getHostIP;
import org.cstor.cproc.cloudComputingFramework.Job;
import org.cstor.cproc.cloudComputingFramework.JobProtocol;
import org.cstor.cproc.cloudComputingFramework.CProcFrameworkProtocol;

public class DoINDEX4 implements JobProtocol{

	public static final Log LOG = LogFactory.getLog(DoINDEX4.class.getName());
	public Configuration confg = null;
	public  ClientProtocol nameNode = null;
	public  ClientDatanodeProtocol dataNode = null;
	public Job job = null;
	HashMap<String, StringBuffer> result = new HashMap<String, StringBuffer>();
	
	public DoINDEX4() {
	}

	public DoINDEX4(DataNode dataNode, Job e) {
		this.job = e;
		this.dataNode = dataNode;
	}

	@Override
	public HashMap<String, StringBuffer> handle() {

		HashMap<String, StringBuffer> DNIPtoFileandOffsets = new HashMap<String, StringBuffer>();
		
		LOG.info("********************************************************************************");
    	LOG.info("*                                                                              *");
    	LOG.info("*                         DoINDEX(test) is running!!!                          *");
    	LOG.info("*                                                                              *");
    	LOG.info("********************************************************************************");

		LOG.info("this job come from " + job.getConf().get("hdfs.job.from.ip"));

		LOG.info("hdfs.job.param.system.param : "
				+ job.getConf().get("hdfs.job.param.system.param") + "-->" + getHostIP.getLocalIP());

		job.getConf().set("hdfs.job.class","org.cstor.cproc.cloudComputingFramework.test.DoSEARCH4");

		job.getConf().set("hdfs.job.from.ip", getHostIP.getLocalIP());

		
		String[] DNIPs = job.getConf().getStrings("hdfs.job.DNIPs");
		for(String DNIp : DNIPs){
			
			DNIPtoFileandOffsets.put(DNIp,(new StringBuffer(job.getConf().get("hdfs.job.param.system.param") +
					"-->" + getHostIP.getLocalIP())));
		}
		
		for(String DNIp : DNIPs){
			LOG.info("DNIPtoFileandOffsets.get("+ DNIp +"):" +DNIPtoFileandOffsets.get(DNIp));
		}
		long sleep = (long)(Math.random()*10);
		while(true){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			LOG.info("-------sleeping:" + sleep + "---------");
			if(sleep == 0){
				break;
			}
			sleep --;
		}

		LOG.info("++++++++++++++++++++++++++++++ DoINDEX4 ++++++++++++++++++++++++++++++");
		return DNIPtoFileandOffsets;
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
	public void setDataNode(ClientDatanodeProtocol dataNode) {
		this.dataNode = dataNode;		
	}

	@Override
	public void setNameNode(ClientProtocol nameNode) {
		this.nameNode = nameNode;		
	}

	public void stop() {
		LOG.info("-----------DoINDEX1 stop!!------------");
	}

	@Override
	public void setCProcFrameworkProtocol(CProcFrameworkProtocol arg0) {
		// TODO Auto-generated method stub
		
	}

}
