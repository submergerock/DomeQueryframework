package org.cstor.cproc.cloudComputingFramework.JobEntity;

import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.net.NetUtils;
import org.cstor.cproc.cloudComputingFramework.CProcFrameworkProtocol;
import org.cstor.cproc.cloudComputingFramework.Job;
public class JobEntity  implements Writable{
	
	String JobId;
	JobEntityType jobEntityType;
	//HashMap<String,JobEntity> subJobEntity = new HashMap<String,JobEntity>();
	
	ConcurrentHashMap<String,JobEntity> subJobEntity = new ConcurrentHashMap<String,JobEntity>();
	
	public static final Log LOG = LogFactory.getLog(JobEntity.class.getName());
	
	boolean printRunning = false;
	boolean changeRunning = false;
	
	private Lock printLock = new ReentrantLock();
	private Lock changeLock = new ReentrantLock();
	
	JobEntity(){
	}
	
	JobEntity(String JobId){
		this.JobId = JobId;
	}
	
	public void setJobId(String JobId) {
		this.JobId = JobId;
	}

	public String getJobId() {
		return this.JobId;
	}
	
	public ConcurrentHashMap<String,JobEntity> getSubJobEntity() {
		return this.subJobEntity;
	}
	
	public void setJobEntityType(JobEntityType jobEntityType) {
		this.jobEntityType = jobEntityType;
	}

	public JobEntityType getJobEntityType() {
		return this.jobEntityType;
	}
	
	public synchronized boolean changeSubJobEntityType(String JobId,JobEntityType jobEntityType) {
		
		String prefixJobId ;
		
		String suffixJobId ;
		
		JobEntity jobEntity = null;
		
		boolean success = true;
		
//		while(changeRunning){
//			try {
//		          wait(100);
//		        } catch (InterruptedException ie) { 
//		        }
//		}
//		
//		synchronized(this){
//			
//			changeRunning = true;
//			
//		}
		//changeLock.lock();
		//001
		//001,192.168.0.1
		
		//001,192.168.0.1,192.168.0.2
		//001,192.168.0.1,192.168.0.3
		
		//001,192.168.0.1,192.168.0.2,192.168.0.2
		//001,192.168.0.1,192.168.0.2,192.168.0.3
		//001,192.168.0.1,192.168.0.3,192.168.0.2
		//001,192.168.0.1,192.168.0.3,192.168.0.4
		
		if(JobId.indexOf(",") == -1){			
			jobEntity = this.subJobEntity.get(JobId);
			
			if(jobEntity == null){
				jobEntity = new JobEntity(JobId);
				if(jobEntityType == JobEntityType.SUCCESS)
					System.out.println("error : jobEntityType == JobEntityType.SUCCESS");
			}
			
			jobEntity.setJobEntityType(jobEntityType);
			this.subJobEntity.put(JobId, jobEntity);
			
			if(jobEntityType == JobEntityType.SUCCESS){
				for(JobEntity jobEntityTmp : this.subJobEntity.values()){
					if(jobEntityTmp.jobEntityType != JobEntityType.SUCCESS){
						success = false;
						break;
					}
				}				
			} else {
				success = false;
			}
			
		} else {
			
			prefixJobId = JobId.substring(0 , JobId.indexOf(","));
			
			suffixJobId = JobId.substring(JobId.indexOf(",") + 1);
			
			jobEntity = this.subJobEntity.get(prefixJobId);
			
			if(jobEntity == null){
				try {
					throw new Throwable("jobEntity == null !!!");
				} catch (Throwable e) {

					e.printStackTrace();
				}
			}
			
			success = jobEntity.changeSubJobEntityType(suffixJobId, jobEntityType);
			
			if(success == true){
				for(JobEntity jobEntityTmp : this.subJobEntity.values()){
					if(jobEntityTmp.jobEntityType != JobEntityType.SUCCESS){
						success = false;
						break;
					}
				}
			} 
			
			
		}
		
		if(success == true)
			this.jobEntityType = JobEntityType.SUCCESS;
		
//		synchronized(this){
//			
//			changeRunning = false;
//			
//		}
		//changeLock.unlock();
		return success;
		
	}
	
	public synchronized JobEntityType getJobEntityType(String JobId){
		
		String prefixJobId ;
		
		String suffixJobId ;
		
		JobEntity jobEntity = null;
		
		if(JobId.indexOf(",") == -1){	
			
			jobEntity = this.subJobEntity.get(JobId);
			
			return jobEntity.getJobEntityType();			
		
		} else {
			
			prefixJobId = JobId.substring(0 , JobId.indexOf(","));
			
			suffixJobId = JobId.substring(JobId.indexOf(",") + 1);
			
			jobEntity = this.subJobEntity.get(prefixJobId);
			
			return jobEntity.getJobEntityType(suffixJobId);
		}
		
	}
	
	public synchronized void printJobEntityType(String JobId,boolean first,String useTime){
		//LOG.info("print JobId ***********222*******");
		if(first == false)
			JobId = JobId + "," + this.JobId;			
		
		LOG.info(JobId + "<->" + this.jobEntityType );//+ "<->useTime : " + useTime);		
		String sql = JobId + "<->" + this.jobEntityType;
		try {
			WriteSQL(sql);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//printLock.lock();
//		while(printRunning){
//			try {
//		          wait(100);
//		        } catch (InterruptedException ie) { 
//		        }
//		}
//		
//		synchronized(this){
//			
//			printRunning = true;
//			
//		}
		//depth--;
		
		//if(depth == 0)
			//return;
		Collection<JobEntity> JobEntitys = null;
		JobEntity[] JobEntityss = null ;
//		while(true){
//			try{
//				JobEntitys = this.subJobEntity.values();
//				
//				JobEntityss = new JobEntity[JobEntitys.size()];
//				int i = 0;
//				for(JobEntity j : JobEntitys){
//					JobEntityss[i++] = j;
//				}
//				break;
//			} catch(ConcurrentModificationException e){
//				try {
//			          wait(100);
//			        } catch (InterruptedException ie) { 
//			        }
//			        continue;
//			}
//		}
		
//		while(true){
//			try{
		//LOG.info("---------JobEntityss is not HashMap!!!-----------");
				for(JobEntity jobEntityTmp : this.subJobEntity.values()){			
					
					jobEntityTmp.printJobEntityType(JobId,false,useTime);
					
				}	
		
//				break;
//			} catch(ConcurrentModificationException e){
//				try {
//			          wait(10);
//			          
//			        } catch (InterruptedException ie) { 
//			        }
//			        continue;
//			}
//		}
		//LOG.info("++++++++++JobEntityss is not HashMap!!!++++++++++");
//		synchronized(this){			
//			printRunning = false;			
//		}	
		//printLock.unlock();
	}
		
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		JobEntity jobEntity = new JobEntity("1");
		
		jobEntity.setJobEntityType(JobEntityType.RUNNING);
		
		jobEntity.changeSubJobEntityType("1", JobEntityType.RUNNING);
		
		jobEntity.changeSubJobEntityType("2", JobEntityType.RUNNING);
		
		jobEntity.changeSubJobEntityType("1,1", JobEntityType.RUNNING);
		
		jobEntity.changeSubJobEntityType("1,2", JobEntityType.RUNNING);
		
		jobEntity.changeSubJobEntityType("2,1", JobEntityType.RUNNING);
		
		jobEntity.changeSubJobEntityType("2,2", JobEntityType.SUCCESS);
		
		System.out.println("1<--->" + jobEntity.getJobEntityType("1"));
		
		System.out.println("2<--->" + jobEntity.getJobEntityType("2"));
		
		System.out.println("1,1<--->" + jobEntity.getJobEntityType("1,1"));
		
		System.out.println("1,2<--->" + jobEntity.getJobEntityType("1,2"));
		
		System.out.println("2,1<--->" + jobEntity.getJobEntityType("2,1"));
		
		System.out.println("2,2<--->" + jobEntity.getJobEntityType("2,2"));
		
		
		
		
		
		
//		String JobId = new String("1234,1,3,4,6");
//		
//		System.out.println("JobId.indexOf(\",\") : " + JobId.indexOf(","));        
//		
//		String prefixJobId = JobId.substring(0 , JobId.indexOf(","));
//		
//		String suffixJobId = JobId.substring(JobId.indexOf(",") + 1);
//		
//		System.out.println("prefixJobId : " + prefixJobId);
//		
//		System.out.println("suffixJobId : " + suffixJobId);
//		
//		HashMap<String,String> subJobEntity = new HashMap<String,String>();
//		
//		subJobEntity.put("1", "1111");
//		
//		System.out.println("subJobEntity.get(\"1\") : " + subJobEntity.get("1"));
//		
//		subJobEntity.put("1", "2222");
//		
//		System.out.println("subJobEntity.get(\"1\") : " + subJobEntity.get("2"));
		
	}

	public static void WriteSQL(String sql) throws IOException{
		File file = new File("/dinglicom/cproc/cproclog/logs.txt");
		if(!file.exists()){
			file.createNewFile();
		}
		BufferedWriter output = new BufferedWriter(new FileWriter(file,true));
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
		String out = df.format(new Date()) + "||-----||" + sql;// new Date()为获取当前系统时间
		output.write(out);
		output.write("\n");
		output.flush();
		output.close();
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
		LOG.info("--------readFields---------");
		System.out.println("--------readFields---------");
		
		this.JobId = arg0.readUTF();
		LOG.info("this.JobId : " + this.JobId);
		System.out.println("this.JobId : " + this.JobId);
		int jType = arg0.readInt();
		LOG.info("jType : " + jType);
		System.out.println("jType : " + jType);
		if(jType == 0){
			this.jobEntityType = JobEntityType.INDEX;
		} else if(jType == 1){
			this.jobEntityType = JobEntityType.INNDIR;
			
		} else if(jType == 2){
			this.jobEntityType = JobEntityType.RUNNING;
		} else if(jType == 3){
			this.jobEntityType = JobEntityType.SEARCH;
		} else if(jType == 4){
			this.jobEntityType = JobEntityType.SUBMITED;
		} else if(jType == 5){
			this.jobEntityType = JobEntityType.SUCCESS;
		} else if(jType == 6){
			this.jobEntityType = JobEntityType.WAITING;
		}
		
		int size = arg0.readInt();
		LOG.info("size : " + size);
		System.out.println("size : " + size);
		
		for(int i = 0 ; i < size ; i++){
			
			String subJobID = arg0.readUTF();
			LOG.info("subJobID : " + subJobID);
			System.out.println("subJobID : " + subJobID);
			JobEntity subJobEntity = new JobEntity();
			subJobEntity.readFields(arg0);
			
			this.subJobEntity.put(subJobID, subJobEntity);
		}
		
		LOG.info("++++++readFields++++++");
		System.out.println("++++++readFields++++++");
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
		LOG.info("--------write---------");
		
		arg0.writeUTF(this.JobId);
		
		LOG.info("this.JobId : " + this.JobId);
		
		LOG.info("this.jobEntityType : " + this.jobEntityType);
		
		if(jobEntityType == JobEntityType.INDEX){
			arg0.writeInt(0);
		} else if(jobEntityType == JobEntityType.INNDIR){
			arg0.writeInt(1);
		} else if(jobEntityType == JobEntityType.RUNNING){
			arg0.writeInt(2);
		} else if(jobEntityType == JobEntityType.SEARCH){
			arg0.writeInt(3);
		} else if(jobEntityType == JobEntityType.SUBMITED){
			arg0.writeInt(4);
		} else if(jobEntityType == JobEntityType.SUCCESS){
			arg0.writeInt(5);
		} else if(jobEntityType == JobEntityType.WAITING){
			arg0.writeInt(6);
		}
		arg0.writeInt(subJobEntity.size());
		
		LOG.info("this.subJobEntity.size() : " + this.subJobEntity.size());
		
		
		for(String subJobID : subJobEntity.keySet()){
			arg0.writeUTF(subJobID);
			LOG.info("subJobID : " + subJobID);
			subJobEntity.get(subJobID).write(arg0);
		}
		LOG.info("+++++++++write+++++++++");
	}
//	
//	
//	public interface testProtocol extends VersionedProtocol{		  
//		  
//		  public JobEntity getJobEntity(JobEntity jobEntity);
//		  
//	}
//	
//	public class test  implements testProtocol, Runnable{
//
//		public test(){
//			InetSocketAddress socAddr = NetUtils.createSocketAddr("0.0.0.0:2222");
//			
//		    // create rpc server 
//			Server server = null;  
//		    try {
//				server = RPC.getServer(this, socAddr.getHostName(), socAddr.getPort(),
//						1, false, new Configuration());
//				server.start();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			
//		}
//		
//		
//		@Override
//		public long getProtocolVersion(String protocol, long clientVersion)
//				throws IOException {
//			// TODO Auto-generated method stub
//			if (protocol.equals(VersionedProtocol.class.getName())) {
//			      return CProcFrameworkProtocol.versionID; 
//			    } else {
//			      throw new IOException("Unknown protocol to CProcFramework node: " + protocol);
//			    }
//		    }
//
//		@Override
//		public JobEntity getJobEntity(JobEntity jobEntity) {
//			// TODO Auto-generated method stub
//			return jobEntity;
//		}
//
//
//		@Override
//		public void run() {
//			// TODO Auto-generated method stub
//			InetSocketAddress socAddr = NetUtils.createSocketAddr("0.0.0.0:2222");
//			
//		    // create rpc server 
//			Server server = null;  
//		    try {
//				server = RPC.getServer(this, socAddr.getHostName(), socAddr.getPort(),
//						1, false, new Configuration());
//				server.start();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		    
//		    
//		    
//		}
//	}
//
//		
//		
//	
	
	
}
