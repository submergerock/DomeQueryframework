package org.cstor.cproc.cloudComputingFramework;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
/**
 * @author WangLei 
 * @version 0.4  
 * Filename :  DistributeData.java
 * Copyright : Copyright 2012 Cstor Tech. Co. Ltd. All Rights Reserved.
 * Describe : 
 * Created time : 2012-5-22
 */
/**********************************************************************
 * This static class owns several thread which dispatchs data to only one 
 * DataNode.
 **********************************************************************/
public class DistributeData {
	public static final Log LOG = LogFactory.getLog(DistributeData.class.getName());
	private static ConcurrentHashMap<String,DistributeToDataNode> DNtoProxy 
	 						= new ConcurrentHashMap<String,DistributeToDataNode>();
	
	private static ConcurrentHashMap<String,CProcFrameworkProtocol> DNtoRPC 
							= new ConcurrentHashMap<String,CProcFrameworkProtocol>();
	
	private static JobToParam jobtoparam =null;
	DistributeData(){		
	}
	
		
	public static void Distribute(Job j,String DN ,RpcConfiguration rpcConf,String param){
		synchronized(DNtoProxy){
			DistributeToDataNode distributeToDataNode = DNtoProxy.get(DN);	
			//LOG.info("distributeToDataNode == null");
			if(distributeToDataNode == null){
				distributeToDataNode = new DistributeToDataNode(DN,rpcConf);
			}
			DNtoProxy.put(DN, distributeToDataNode);
			jobtoparam = new JobToParam(j,param);
			distributeToDataNode.putJob(jobtoparam);
//			jobtoparam.setJob(null);
//			jobtoparam.setParam("");
		}
		//LOG.info("instence sum : " + DNtoProxy.size() );
	}
	
	
	
	/**********************************************************************
	 * Dispatch data thread.
	 **********************************************************************/
	private static class DistributeToDataNode implements Runnable{
		private ArrayBlockingQueue<JobToParam> jobQueue = new ArrayBlockingQueue<JobToParam>(10*1024);
		private String DN;
		private RpcConfiguration rpcConf;
		CProcFrameworkProtocol DistributeNode = null;
		volatile private boolean running = true;
		
		DistributeToDataNode(String DN ,RpcConfiguration rpcConf){
			this.DN = DN;
			this.rpcConf = rpcConf;
			initialize(DN ,rpcConf);			
		}
		private void initialize(String DN ,RpcConfiguration rpcConf){
			String DistributeNodeIP = DN.split(":")[0] + ":" + CProcFramework.port;
			LOG.info("DistributeNodeIP : " + DistributeNodeIP);
			try {
				DistributeNode = (CProcFrameworkProtocol)RPC.getProxy(CProcFrameworkProtocol.class,
						CProcFrameworkProtocol.versionID, NetUtils.createSocketAddr(DistributeNodeIP), this.rpcConf,
				        NetUtils.getSocketFactory(this.rpcConf, CProcFrameworkProtocol.class));
			} catch (IOException e) {
				e.printStackTrace();
			}
//			finally{
//				//20131210修改RPC关闭
//				RPC.stopProxy(DistributeNode);
//			}
			Thread jobControlThread = new Thread(this);
			jobControlThread.start();
		}
		public void putJob(JobToParam e){
			   try {
				this.jobQueue.put(e);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		   }
		  private JobToParam takeJob(){
			   JobToParam e = null;
			   try {
				e = this.jobQueue.take();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			return e;
		   }
		@Override
		public void run() {
			LOG.info("************************************************************");
	    	LOG.info("*  DistributeToDataNode : "+ this.DN +" is running!!! *");
	    	LOG.info("************************************************************");
			JobToParam jobToParam = null;
			
			 while (running) {
				 jobToParam = takeJob();
				 Job j = jobToParam.getJob();
				 String param = jobToParam.getParam();
				 synchronized (j.getConf()) {
				 j.getConf().set("hdfs.job.param.system.param", param);
//				 System.gc();
				 DistributeNode.submitJob(j,new Text(param));
//				 System.gc();
				 LOG.info("Distribute to " + this.DN);
				 //2013.7.30
				 
					 j.getConf().set("hdfs.job.param.system.param", "");

				}
//				 j = null;
				 param = null;
			  }
		}
	}
	
	private static class JobToParam{
		Job j;
		String param;
		JobToParam(Job j,String param){
			this.j = j;
			this.param = param;
		}
		public void setJob(Job j){
			this.j = j;
		}
		
		public Job getJob(){
			return this.j;
		}
		
		public void setParam(String param){
			this.param = param;
		}
		
		public String getParam(){
			return this.param;
		}
	}
	
}

