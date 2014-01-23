package org.cstor.cproc.cloudComputingFramework.JobEntity;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Hashtable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cstor.cproc.cloudComputingFramework.CProcFramework;


public class JobEntityStore {
	
	public static final Log LOG = LogFactory.getLog(JobEntityStore.class.getName());
	
	
	ConcurrentHashMap<String,JobEntity> subJobEntity = new ConcurrentHashMap<String,JobEntity>();
	
	private int depthOfPrintJobEntityType = 3;
	
	private Object lock = new Object();
	public JobEntityStore(int depthOfPrintJobEntityType){
		this.depthOfPrintJobEntityType = depthOfPrintJobEntityType;
	}

//wzt ??? JobEntityType 的例子
public synchronized void changeJobEntityType(String JobId,JobEntityType jobEntityType) {
		
	
		//LOG.info("change JobId \"" + JobId + "\" to \"" + jobEntityType + "\"!!!");
	
		String prefixJobId ;
		
		String suffixJobId ;
		
		JobEntity jobEntity = null;		
		
		if(JobId.indexOf(",") == -1){	
			
			
			synchronized (lock) {
			
			jobEntity = this.subJobEntity.get(JobId);
			
			
				
			if(jobEntity == null)
				jobEntity = new JobEntity(JobId);
			jobEntity.setJobEntityType(jobEntityType);
			this.subJobEntity.put(JobId, jobEntity);
			
			}
			
			//
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
			
			jobEntity.changeSubJobEntityType(suffixJobId, jobEntityType);			
			
		}		
	}
	
	public synchronized JobEntityType getJobEntityType(String JobId){
		
		String prefixJobId ;
		
		String suffixJobId ;
		
		JobEntity jobEntity = null;
		
		if(JobId.indexOf(",") == -1){	
			
			jobEntity = this.subJobEntity.get(JobId);
			
			if(jobEntity == null)
				return null;
			
			return jobEntity.getJobEntityType();			
		
		} else {
			
			prefixJobId = JobId.substring(0 , JobId.indexOf(","));
			
			suffixJobId = JobId.substring(JobId.indexOf(",") + 1);
			
			jobEntity = this.subJobEntity.get(prefixJobId);
			
			return jobEntity.getJobEntityType(suffixJobId);
		}
		
	}
	
public synchronized JobEntity getJobEntity(String JobId){
	
	return	this.subJobEntity.get(JobId);
	}
	
	public synchronized void printJobEntityType(String JobId,String useTime){
		
		//LOG.info("print JobId \"" + JobId + "\" jobEntityType !!!");
		
		String prefixJobId ;
		
		String suffixJobId ;
		
		JobEntity jobEntity = null;
		
//		if(JobId.indexOf(",") == -1){
		
			jobEntity = this.subJobEntity.get(JobId);
			
			//LOG.info("print JobId ***********111*******");
			
			//LOG.info("print JobId : " + JobId + "|  JobEntityType : " +jobEntity.getJobEntityType());
			
			jobEntity.printJobEntityType(JobId,true,useTime);	
		
			
			//jobEntity.printJobEntityType(JobId,true,this.depthOfPrintJobEntityType);
//		} else {
//			
//			prefixJobId = JobId.substring(0 , JobId.indexOf(","));
//			
//			suffixJobId = JobId.substring(JobId.indexOf(",") + 1);
//			
//			jobEntity = this.subJobEntity.get(prefixJobId);
//			
//			jobEntity.printJobEntityType(suffixJobId,true);
//			
//		}
		
	}
	
	public static void main(String[] args) {		

		JobEntityStore jobEntityStore = new JobEntityStore(3);
		
		jobEntityStore.changeJobEntityType("1", JobEntityType.RUNNING);
		
//		System.out.println("1<--->" + jobEntityStore.getJobEntityType("1"));
		
		jobEntityStore.changeJobEntityType("2", JobEntityType.RUNNING);		
//
//		System.out.println("1<--->" + jobEntityStore.getJobEntityType("1"));
//		
//		System.out.println("2<--->" + jobEntityStore.getJobEntityType("2"));
		
		jobEntityStore.changeJobEntityType("1,1", JobEntityType.RUNNING);
		
		jobEntityStore.changeJobEntityType("1,2", JobEntityType.RUNNING);
		
		jobEntityStore.changeJobEntityType("2,1", JobEntityType.RUNNING);
		
		jobEntityStore.changeJobEntityType("2,2", JobEntityType.RUNNING);
		
		
		jobEntityStore.changeJobEntityType("1,1,1", JobEntityType.RUNNING);
		
		jobEntityStore.changeJobEntityType("1,1,2", JobEntityType.RUNNING);
		
		jobEntityStore.changeJobEntityType("1,2,1", JobEntityType.RUNNING);
		
		jobEntityStore.changeJobEntityType("1,2,2", JobEntityType.RUNNING);
		
		jobEntityStore.changeJobEntityType("2,1,1", JobEntityType.RUNNING);
		
		jobEntityStore.changeJobEntityType("2,1,2", JobEntityType.RUNNING);
		
		jobEntityStore.changeJobEntityType("2,2,1", JobEntityType.RUNNING);
		
		jobEntityStore.changeJobEntityType("2,2,2", JobEntityType.RUNNING);
		
		
		jobEntityStore.printJobEntityType("1","");
		
		
		jobEntityStore.changeJobEntityType("1,1,1", JobEntityType.SUCCESS);
		
		jobEntityStore.changeJobEntityType("1,1,2", JobEntityType.SUCCESS);
//		
		jobEntityStore.changeJobEntityType("1,2,1", JobEntityType.SUCCESS);
//		
		jobEntityStore.changeJobEntityType("1,2,2", JobEntityType.SUCCESS);
//		
		jobEntityStore.changeJobEntityType("2,1,1", JobEntityType.SUCCESS);
//		
		jobEntityStore.changeJobEntityType("2,1,2", JobEntityType.SUCCESS);
//		
		jobEntityStore.changeJobEntityType("2,2,1", JobEntityType.SUCCESS);
//		
		jobEntityStore.changeJobEntityType("2,2,2", JobEntityType.SUCCESS);
		
		jobEntityStore.printJobEntityType("1","");
		
//		System.out.println("1<--->" + jobEntityStore.getJobEntityType("1"));
//		
//		System.out.println("2<--->" + jobEntityStore.getJobEntityType("2"));
//		
//		System.out.println("1,1<--->" + jobEntityStore.getJobEntityType("1,1"));
//		
//		System.out.println("1,2<--->" + jobEntityStore.getJobEntityType("1,2"));
//		
//		System.out.println("2,1<--->" + jobEntityStore.getJobEntityType("2,1"));
//		
//		System.out.println("2,2<--->" + jobEntityStore.getJobEntityType("2,2"));
//		
//		
//		
//		System.out.println("1,1,1<--->" + jobEntityStore.getJobEntityType("1,1,1"));
//		
//		System.out.println("1,1,2<--->" + jobEntityStore.getJobEntityType("1,1,2"));
//		
//		System.out.println("1,2,1<--->" + jobEntityStore.getJobEntityType("1,2,1"));
//		
//		System.out.println("1,2,2<--->" + jobEntityStore.getJobEntityType("1,2,2"));
//		
//		System.out.println("2,1,1<--->" + jobEntityStore.getJobEntityType("2,1,1"));
//		
//		System.out.println("2,1,2<--->" + jobEntityStore.getJobEntityType("2,1,2"));
//		
//		System.out.println("2,2,1<--->" + jobEntityStore.getJobEntityType("2,2,1"));
//		
//		System.out.println("2,2,2<--->" + jobEntityStore.getJobEntityType("2,2,2"));
	}

}
