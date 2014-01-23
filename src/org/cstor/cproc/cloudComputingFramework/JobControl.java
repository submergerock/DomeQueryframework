package org.cstor.cproc.cloudComputingFramework;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.job.tools.getHostIPcProc;
import org.apache.hadoop.net.NetUtils;
import org.cstor.cproc.cloudComputingFramework.JobEntity.JobEntityType;

public class JobControl implements Runnable{

	 public static final Log LOG = LogFactory.getLog(JobControl.class.getName());
	 private ArrayBlockingQueue<Job> jobQueue = new ArrayBlockingQueue<Job>(10*1024);

	 private ConcurrentHashMap<String,RunningJob> jobidToRunningJob = new ConcurrentHashMap<String,RunningJob>();
	 volatile private boolean running = true;
	 public  String network = null; 
	 private boolean isNN = true;
	 private String DNIPs = null;
	 private CProcConfiguration cprocConf; // configuration of local standby namenode
	 private RpcConfiguration rpcConf; // configuration of local standby namenode
	 private ClientProtocol nameNode = null;
	 private ClientDatanodeProtocol dataNode = null;
	 private CProcFrameworkProtocol CProcFrameworkNode = null;
	 
	 private ExecutorService executorService = null; 
	 private InetSocketAddress nameNodeAddr = null;
	 
	  
	 public JobControl(ClientProtocol nameNode , ClientDatanodeProtocol dataNode ,
			 CProcConfiguration cprocConf,RpcConfiguration rpcConf , boolean isNN,String network,
			 InetSocketAddress nameNodeAddr){
		 
		  this.cprocConf = cprocConf;
		  
		  this.rpcConf = rpcConf;
		  
		  this.nameNode = nameNode;
		  
		  this.dataNode = dataNode;
		  
		  this.isNN = isNN;
		  
		  this.network = network;
		  
		  this.nameNodeAddr = nameNodeAddr;
		  
		  this.executorService = Executors.newFixedThreadPool(this.rpcConf.getInt("cproc.threadpool.handler.count",128));
		  
		  String CProcFrameworkNodeIP = CProcFramework.namenodeAddress.split(":")[0] 
		                                                     + ":" + CProcFramework.port;
	      
	      LOG.info("CProcFrameworkNodeIP == " + CProcFrameworkNodeIP);
			 
	      InetSocketAddress CProcFrameworkNodeAddr = NetUtils.createSocketAddr(CProcFrameworkNodeIP);			 
				
		  try {
			this.CProcFrameworkNode = (CProcFrameworkProtocol)RPC.getProxy(CProcFrameworkProtocol.class,
					CProcFrameworkProtocol.versionID, CProcFrameworkNodeAddr, this.rpcConf,
			        NetUtils.getSocketFactory(this.rpcConf, CProcFrameworkProtocol.class));
		} catch (IOException e) {
			e.printStackTrace();
		}
//		finally{
//			//20131210修改RPC关闭
//			RPC.stopProxy(this.CProcFrameworkNode);
//		}
		  
	 }
	  
	 public void run() {
		  LOG.info("JobControl thread startup!!!");
		  Job j = null;
		  boolean isEnd[] = new boolean[1];
		  isEnd[0] = false;
		  
	    while (running) {
	    	
	    	j = takeJob();
	    	if(j!=null){
	    		runJob(j,isEnd);
	    	}
//	    	tools.sleep(50); 
	    	LOG.info("isEnd[0] == " + isEnd[0]);  
			  
			  isEnd[0] = false;
	    	LOG.info("******************************************");
	    	LOG.info("*          JobControl is running!!!      *");
	    	LOG.info("******************************************");
	    }
	  }

	  private void runJob(Job j,boolean isEnd[]){
		  LOG.info("-----JobControl.runJob(Job j)-----");
		  //wzt master,slave 最大的区别是 master能改变job的工作状态不实际运行job
		  //slave 实际运行 job，他向master提交job的工作状态
		  //但我没有看到 job 的转发
		  //in NN,find all DNIP and set them in conf
		  if( this.isNN == true ){
			  DatanodeDescriptor[] datanodeDescriptors = null;
			try {
				datanodeDescriptors  =  this.nameNode.getAllDatanode();
				synchronized (j.getConf()) {
					j.getConf().setInt("hdfs.job.DNTotal", datanodeDescriptors.length);
					  
					j.getConf().set("hdfs.job.NN.ip", getHostIPcProc.getLocalIP( this.network) + ":" +this.nameNode.getNameNodeAddressPort());
				} 
			} catch (IOException e) {
				e.printStackTrace();
			}
			  
			  String DNIPs = "";
			  for(DatanodeDescriptor datanodeDescriptor : datanodeDescriptors){
				  if(DNIPs == ""){
					  DNIPs = DNIPs + datanodeDescriptor.getHost() 
							  + ":" + datanodeDescriptor.getIpcPort();
				  } else {
					  DNIPs = DNIPs + "," + datanodeDescriptor.getHost() 
							  + ":" + datanodeDescriptor.getIpcPort();
				  }
			  }
			  synchronized (j.getConf()) {
				  j.getConf().set("hdfs.job.DNIPs",DNIPs);
				  
				  j.getConf().set("hdfs.job.jobid",j.getJobId().toString());
				  
//				  this.CProcFrameworkNode.changeJobEntityType(j.getConf().get("hdfs.job.jobid") , JobEntityType.RUNNING);
				  
				  j.getConf().set("hdfs.job.jobid",j.getJobId().toString() + "," + getHostIPcProc.getLocalIP(this.network));
				  
				  this.CProcFrameworkNode.changeJobEntityType(j.getConf().get("hdfs.job.jobid") 
						  , JobEntityType.RUNNING);
//				  if(j.getConf().getBoolean("hdfs.job.jobid.print",false))
//				  this.CProcFrameworkNode.printJobEntityType(j.getJobId().toString(),"");
			  }
			  LOG.info("datanodeDescriptors.size() : " + datanodeDescriptors.length);
			  LOG.info("DNIPs : " + DNIPs);
		  } else {
			  synchronized (j.getConf()) {
				  j.getConf().set("hdfs.job.jobid",j.getConf().get("hdfs.job.jobid") + "," + getHostIPcProc.getLocalIP(this.network));
			  }
		  }
		
		  LOG.info("j.getJobName() : " + j.getJobName());
		  LOG.info("j.getJobId() : " + j.getJobId());  	
		  
//		  RPC.stopProxy(this.nameNode);
		  
		  try {
			this.nameNode = (ClientProtocol) RPC.getProxy(ClientProtocol.class,
						ClientProtocol.versionID, this.nameNodeAddr, this.rpcConf, NetUtils
								.getSocketFactory(this.rpcConf, ClientProtocol.class));
			} catch (IOException e) {
				e.printStackTrace();
			}
//			finally{
//				//20131210修改RPC关闭
//				RPC.stopProxy(this.nameNode);
//			}
		  
		  
		  RunningJob runningJob = new RunningJob(j,this.CProcFrameworkNode
				  ,this.cprocConf,this.rpcConf
				  ,this.nameNode,this.dataNode,this.isNN,isEnd);
		 		  
//		  RPC.stopProxy(this.nameNode);
		  
//		  jobidToRunningJob.put(j.getConf().get("hdfs.job.jobid"), runningJob);
		  
		  //long startTime = System.currentTimeMillis();
		  executorService.execute(runningJob); 
		  //2013.7.30
//		  this.CProcFrameworkNode.printJobEntityType(j.getJobId().toString(),"123123");
		  //long endTime = System.currentTimeMillis();
		 // LOG.info("wl---------------------------RunningJob   use time :" + (endTime - startTime));
		
		  //runningJob.getCurrentThreadInstance().destroy()
		  runningJob = null;		  
		  j = null;
		  
		  //System.gc();
		
		 LOG.info("+++++JobControl.runJob(Job j)+++++");
	  }

	  public void putJob(Job e){
		   try {
			this.jobQueue.put(e);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
	   }
	  private Job takeJob(){
		   Job e = null;
		   try {
			e = this.jobQueue.take();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		return e;
	   }
//	  
//	  public ConcurrentHashMap<String,Thread> getJobidToThread(){
//		  return this.jobidToThread;
//	  }
//	  
	  public void stopJob(String jobid){
			for(String jid : this.jobidToRunningJob.keySet()){
				if(jid.split(",")[0].equals(jobid)){
					RunningJob  runningJob = this.jobidToRunningJob.get(jid);
					runningJob.stop();
					LOG.info("------------stopping job : " + jid  + "------------");
					if(this.isNN){
						stopDNJob(jobid);
					}
				}
			}
			this.CProcFrameworkNode.changeJobEntityType(jobid , JobEntityType.SUCCESS);			  
//			this.CProcFrameworkNode.printJobEntityType(jobid,"");
		}
	  private String getDNIPs(){
		  DatanodeDescriptor[] datanodeDescriptors = null;
			
				try {
					datanodeDescriptors  =  this.nameNode.getAllDatanode();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
			  String DNIPs = "";
			  for(DatanodeDescriptor datanodeDescriptor : datanodeDescriptors){
				  if(DNIPs == ""){
					  DNIPs = DNIPs + datanodeDescriptor.getHost() + ":" + datanodeDescriptor.getIpcPort();
				  } else {
					  DNIPs = DNIPs + "," + datanodeDescriptor.getHost() + ":" + datanodeDescriptor.getIpcPort();
				  }
			  }
			  return DNIPs;			  
	  }
	  private void stopDNJob(String jobid){

		  CProcFrameworkProtocol CProcFrameworkNode = null;
		  InetSocketAddress CProcFrameworkNodeAddr = null;
		  
		  String NodePort = this.rpcConf.get("cproc.node.port","8888");
		  String CProcFrameworkNodeIP = null;
		  
		  for(String DN : getDNIPs().split(",")){
			  CProcFrameworkNodeIP = DN.split(":")[0] + ":" + NodePort;
				 CProcFrameworkNodeAddr = NetUtils.createSocketAddr(CProcFrameworkNodeIP);
				 
					try {
						CProcFrameworkNode = (CProcFrameworkProtocol)RPC.getProxy(CProcFrameworkProtocol.class,
								CProcFrameworkProtocol.versionID, CProcFrameworkNodeAddr, this.rpcConf,
						        NetUtils.getSocketFactory(this.rpcConf, CProcFrameworkProtocol.class));
						LOG.info("CProcFrameworkNodeAddr : " + CProcFrameworkNodeAddr);					
													
						CProcFrameworkNode.stopJob(jobid);
					} catch (IOException e) {
						e.printStackTrace();
					} 
//					finally {
//						//20131210修改RPC关闭
//						RPC.stopProxy(CProcFrameworkNode);
//					}
					
					
			 }
	  }
}
