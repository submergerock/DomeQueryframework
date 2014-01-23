package org.cstor.cproc.cloudComputingFramework.test;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.job.tools.getHostIP;
import org.apache.hadoop.job.tools.tools;
import org.apache.hadoop.net.NetUtils;
import org.cstor.cproc.cloudComputingFramework.CProcFramework;
import org.cstor.cproc.cloudComputingFramework.CProcFrameworkProtocol;
import org.cstor.cproc.cloudComputingFramework.Job;

public class test_invalidnode {
	public static final Log LOG = LogFactory.getLog(test_invalidnode.class.getName());
	
	
	/**
	 * @param args
	 */
	public static void main(String[] argv) {
		String s = "192.168.137.11:9000";
		InetSocketAddress nameNodeAddr = null;
		ClientProtocol nameNode = null;
		try{
  		  nameNodeAddr = NetUtils.createSocketAddr(s);
  		  Configuration conf = new Configuration();
		  nameNode = (ClientProtocol) RPC.getProxy(ClientProtocol.class,
					ClientProtocol.versionID, nameNodeAddr, conf, NetUtils
							.getSocketFactory(conf, ClientProtocol.class));
  	  } catch (IllegalArgumentException e){
  		  //nameNodeAddr = NetUtils.createSocketAddr(argv[0]);
  	  } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	  }
		DatanodeDescriptor[] results = nameNode.getInvalidDNs(10);
		for(DatanodeDescriptor dn : results){
			LOG.info("dn.getHost() : " + dn.getHost());
			//LOG.info("DN : " + dn...getHost());
		}
			
	}
}
