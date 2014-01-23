package org.cstor.cproc.cloudComputingFramework.test;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.job.tools.getHostIP;
import org.apache.hadoop.job.tools.tools;
import org.apache.hadoop.net.NetUtils;
import org.cstor.cproc.cloudComputingFramework.CProcFramework;
import org.cstor.cproc.cloudComputingFramework.CProcFrameworkProtocol;
import org.cstor.cproc.cloudComputingFramework.Job;

public class test_cprocframework {
	public static final Log LOG = LogFactory.getLog(test_cprocframework.class.getName());
	
	private static void test_framework(String[] argv){
		Configuration conf = new Configuration();
		String namenodeAddress = null;
		  if(argv.length >= 1){
			  namenodeAddress = argv[0];	  		
	  	  } else {	  		  
	  			LOG.info("test_cprocframeworktest_cprocframework [namenode Address : port]");
		  		System.exit(0);
	  	  }
		
		System.out.println("getHostIP.getLocalIP() : " + getHostIP.getLocalIP());
		Job job = new Job();
		job.getConf().set("hdfs.job.jar","InnerCDR4");
		job.getConf().set("hdfs.job.class","org.cstor.cproc.cloudComputingFramework.test.DoNNDIR4");
		job.getConf().set("hdfs.job.from.ip",getHostIP.getLocalIP());
		job.getConf().set("hdfs.job.param.sessionId","000");
	
		job.setJobName(new Text("myjob"));
		
		CProcFrameworkProtocol frameworkNode = null;
		InetSocketAddress frameworkNodeAddr = NetUtils.createSocketAddr(namenodeAddress);
	
		try {
			frameworkNode = (CProcFrameworkProtocol)RPC.getProxy(CProcFrameworkProtocol.class,
					CProcFrameworkProtocol.versionID, frameworkNodeAddr, conf,
			        NetUtils.getSocketFactory(conf, CProcFrameworkProtocol.class));		
		
			frameworkNode.submitJob(job);
		} catch (IOException e) {
			e.printStackTrace();
		}
	
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] argv) {
		String s = "192.168.137.11:8888";
		String[] argv1 = new String[1];
		argv1[0] = s;
		test_framework(argv1);
	}
}
