package org.cstor.cproc.cloudComputingFramework;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.io.Text;
import org.cstor.cproc.cloudComputingFramework.JobEntity.JobEntityType;


public class RunningJob  implements Runnable{
	
	  public static final Log LOG = LogFactory.getLog(CProcFramework.class.getName());
	  
	  private Job j = null;

	  private CProcConfiguration cprocConf; // configuration of local standby namenode
	  private RpcConfiguration rpcConf; // configuration of local standby namenode
	  private ClientProtocol nameNode = null;
	  private ClientDatanodeProtocol dataNode = null;
	  private CProcFrameworkProtocol CProcFrameworkNode = null;
	  private boolean stop = false;
	  private ArrayList<String> pram = new ArrayList<String>();
	  
	  private boolean isNN = true;
	  
	  boolean isEnd[];
	                
	  //private final String NodePort ;
	  
	  //private ExecutorService distributeService = null;
	  
	  JobProtocol doJob = null;
	  
	  URL xUrl;
	  URLClassLoader ClassLoader;
	  Class xClass;
	  
	  RunningJob(Job j, CProcFrameworkProtocol CProcFrameworkNode ,
			  CProcConfiguration cprocConf, RpcConfiguration rpcConf,
			  ClientProtocol nameNode, ClientDatanodeProtocol dataNode, 
			  boolean isNN, boolean isEnd[]){
		  this.j = j;
		  
		  this.CProcFrameworkNode = CProcFrameworkNode;
		  
		  this.cprocConf = cprocConf;
		  
		  this.rpcConf = rpcConf;
		  
		  this.nameNode = nameNode;
		  
		  this.dataNode = dataNode;
		  
		  this.isNN = isNN;
		  
		  this.isEnd = isEnd;
		  
		  //this.NodePort = this.confg.get("cproc.node.port","6666");
		  
		  //this.distributeService = Executors.newFixedThreadPool(this.confg.getInt("cproc.threadpool.handler.count",128));
	  }
	
	//run()   函数需要再讲下
	@Override
	public void run() {	
		LOG.info("--------RunningJob.run()--------");
		
	    //LOG.info("Start a new job !!!" );
	    
	    long startTime = System.currentTimeMillis();
	    //LOG.info("Start a new job !!! start time : " + startTime );
	  
	    HashMap<String,StringBuffer> DNstoJobsParam = null;
	    if(!this.stop)
	   	//LOG.info("*********this.stop = " + this.stop +"******************");
		DNstoJobsParam = handle();
	    //LOG.info("*********this.stop = " + this.stop +"******************");
		if(DNstoJobsParam != null && !this.stop){
			distribute(DNstoJobsParam);
			this.isEnd[0] = false;
		} else {
			this.isEnd[0] = true;
			synchronized (j.getConf()) {
				LOG.info("hdfs.job.jobstatus : " + j.getConf().get("hdfs.job.jobstatus"));
				
				if(!this.j.getConf().get("hdfs.job.jobstatus","true").equals("false")){
					LOG.info("++++++++++ture:"+this.j.getConf().get("hdfs.job.param.system.param"));
			}
			//this.CProcFrameworkNode.changeJobEntityType(j.getConf().get("hdfs.job.jobid"), JobEntityType.SUCCESS);
			//this.CProcFrameworkNode.printJobEntityType(j.getJobId().toString());
			}
			LOG.info("end a job !!!" );
		}
		if(DNstoJobsParam!=null){
			DNstoJobsParam.clear();
		}
		long endTime = System.currentTimeMillis();
		 DNstoJobsParam = null;
		 
		 //System.gc();
		// LOG.info("Finish a  job !!! end time : " + endTime );
		
		LOG.info("+++++RunningJob.run()+++++use time :" + (endTime - startTime));
	}

	public HashMap<String,StringBuffer> handle(){
		 LOG.info("------RunningJob.handle()------");
		 HashMap<String,StringBuffer> DNstoJobsParam = null;
		 String jobClass = "";
		 String jobjar = "";
		 synchronized (this.j.getConf()) {
			 jobClass = this.j.getConf().get("hdfs.job.class");  
			 jobjar = this.j.getConf().get("hdfs.job.jar");
		 }
		 LOG.info("jarFile : " + jobjar);
		 LOG.info("jobClass : " + jobClass);
//		 System.out.println("jarFile : " + jobjar);
//		 System.out.println("jobClass : " + jobClass);
		 if(jobjar==null)
			 jobjar = "/dinglicom/hadoop/lib/CDR_ChinaMobile.jar";
		 File xFile=new File(jobjar); 		 
		 Object xObject;
			try {
				if(xUrl==null){
					xUrl = xFile.toURL();
					ClassLoader=new URLClassLoader(new URL[]{ xUrl });  
					xClass=ClassLoader.loadClass(jobClass); 
				}
				xObject=xClass.newInstance();  				
				if(xObject instanceof JobProtocol ){
					doJob = (JobProtocol) xObject;
				} else {
					LOG.info("xObject is not instanceof JobProtocol!!!");
					return null;
				}
				doJob.setJob(this.j);
		   		doJob.setConfiguration(this.cprocConf);
		   		doJob.setNameNode(nameNode);
		   		doJob.setDataNode(dataNode);    
		   		//doJob.setCProcFrameworkProtocol(CProcFrameworkNode);		   		
//		   		CProcFramework.CProcFrameworkNode.changeJobEntityType(this.j.getJobId().toString(), JobEntityType.RUNNING);
		   		
		   		DNstoJobsParam = doJob.handle();
//		   		System.out.println(doJob.getClass().toString());
		   		//2013.7.18
		   		synchronized (this.j.getConf()) {
		   			this.j.getConf().set("hdfs.job.param.system.param","");
				}
//		   		String jobid = this.j.getConf().get("hdfs.job.jobid");
//		   		j.getConf().clear();
//		   		j.getConf().set("hdfs.job.jobid",jobid);
		   		//20131210新增任务完成后打印日志
		   		this.CProcFrameworkNode.printJobEntityType(this.j.getJobId().toString());
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SecurityException e) {
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			}  
	
 	 LOG.info("+++++RunningJob.handle()+++++");
 	 return DNstoJobsParam;
	}	
	 
//	class DistributeRunning  implements Runnable{
//		private Job j2 = null;
//		String DN  = null;
//		private Configuration confg2 = null;
//		private String param2 = null;
//		CProcFrameworkProtocol DistributeNode = null;
//		
//		InetSocketAddress  DistributeNodeAddr = null;
//		
//		DistributeRunning(Job j,String DN ,Configuration confg,String param){
//			this.j2 = j;
//		
//			//this.CProcFrameworkNodeAddr2 = CProcFrameworkNodeAddr;
//			
//			String DistributeNodeIP = DN.split(":")[0] + ":" + NodePort;
//			
//			DistributeNodeAddr = NetUtils.createSocketAddr(DistributeNodeIP);			
//			
//			this.confg2 = confg;
//			this.param2 = param;
//		}
//
//		@Override
//		public void run() {
//			try {
//				DistributeNode = (CProcFrameworkProtocol)RPC.getProxy(CProcFrameworkProtocol.class,
//						CProcFrameworkProtocol.versionID, this.DistributeNodeAddr, this.confg2,
//				        NetUtils.getSocketFactory(this.confg2, CProcFrameworkProtocol.class));
//				synchronized (this.j2.getConf()) {
//					this.j2.getConf().set("hdfs.job.param.system.param", this.param2);
//					DistributeNode.submitJob(this.j2,new Text(this.param2));
//					this.j2.getConf().set("hdfs.job.param.system.param", "");
//				}
//			} catch (IOException e) {
//				e.printStackTrace();
//			} finally{
//				RPC.stopProxy(DistributeNode);
//			}
//		}
//	}
	
	//wzt  如果job就在本机上，也需要cprocframework 来使用rpc更改状态吗
	 public synchronized void  distribute(HashMap<String,StringBuffer> DNtoJobsParam){		 
		 LOG.info("-----RunningJob.distribute()-----");
//		 CProcFrameworkProtocol CProcFrameworkNode = null;
		 //InetSocketAddress CProcFrameworkNodeAddr = null;		
		 synchronized(DNtoJobsParam){
			 Set<String> DNs = DNtoJobsParam.keySet();//key(datanodeip) value(datanodestring)
			 
			 //String CProcFrameworkNodeIP = null;		 
			 
			 if(DNs.size() == 0)
				 this.isEnd[0] = true;
			 		 
			 for(String DN : DNs){
				 //LOG.info("-----RunningJob.distribute()---000--");
				 synchronized (j.getConf()) {
					 this.CProcFrameworkNode.changeJobEntityType(j.getConf().get("hdfs.job.jobid") 
							   + "," + DN.split(":")[0], JobEntityType.RUNNING);
//					  if(j.getConf().getBoolean("hdfs.job.jobid.print",false))
//					   this.CProcFrameworkNode.printJobEntityType(j.getJobId().toString(),"");
				}
				 // LOG.info("-----RunningJob.distribute()---111--");
			 }			 
			 	 
			 LOG.info("-----DNs.size() : " + DNs.size());
			 for(String DN : DNs){
				 LOG.info("-----DN : " + DN);
								 				 
				 DistributeToDataNodeNoneDaemon distributeToDN = new DistributeToDataNodeNoneDaemon(this.j,
						 DNtoJobsParam.get(DN).toString(),DN,RPCConnections.getRPC(DN, this.rpcConf));
				 
				 Thread distributeToDNThread = new Thread(distributeToDN);
				 distributeToDNThread.start();
				 
			 }
			 //2013.7.17 
			 DNtoJobsParam.clear();
		 }

		 LOG.info("+++++RunningJob.distribute()+++++");
	 }
	 
	 public void stop(){
		this.stop = true;
		if(this.doJob != null){
			this.doJob.stop();
		}
	 }
	 
	 private static class DistributeToDataNodeNoneDaemon implements Runnable{
			
			private Job j;
			private CProcFrameworkProtocol DistributeNode = null;
			private String param = null;
			private String DN = null;
			volatile private boolean running = true;
			
			DistributeToDataNodeNoneDaemon(Job j,String param, String DN,CProcFrameworkProtocol DistributeNode){
				this.j = j;
				this.param = param;
				this.DN = DN;
				this.DistributeNode = DistributeNode;
			}
						
			@Override
			public void run() {
				LOG.info("************************************************************");
		    	LOG.info("*  DistributeToDataNodeNoneDaemon : "+ this.DN +" is running!!! *");
		    	LOG.info("************************************************************");
				
				 while (running) {
					 synchronized (this.j.getConf()) {
						 if(param!=null){
							 this.j.getConf().set("hdfs.job.param.system.param", this.param);
			
							 DistributeNode.submitJob(this.j,new Text(this.param));
			
							 LOG.info("Distribute to " + this.DN);
						 }else{
							 break;
						 }
					}
					 param = null;
				  }
			}
		}
	 	
}


