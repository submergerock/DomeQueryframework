package org.cstor.cproc.cloudComputingFramework;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestCProcFramework {
	public static final Log LOG = LogFactory.getLog(CProcFramework.class.getName());
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		 
		 LOG.info("------TestCProcFramework------");
		 HashMap<String,StringBuffer> DNstoJobsParam = null;
		 JobProtocol doJob = null;
		 
		 String jobClass = "org.apache.hadoop.hdfs.job.framework.DoNNDIR";  
		 File xFile=new File("/usr/hadoop-0.20.2/lib/CDR_ChinaMobile.jar"); 		 
		 
		 LOG.info("jarFile : " + xFile.getPath());
		 LOG.info("jobClass : " + jobClass);		
		 
		 URL xUrl;
		 URLClassLoader ClassLoader;
		 Class xClass;
		 Object xObject;
			
				try {
					xUrl = xFile.toURL();
					ClassLoader=new URLClassLoader(new URL[]{ xUrl });  
					xClass=ClassLoader.loadClass(jobClass);  
					xObject=xClass.newInstance(); 
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				 				
				
	}

}
