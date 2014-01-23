package org.cstor.cproc.cloudComputingFramework;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
public class RPCConnections {
	public static final Log LOG = LogFactory.getLog(RPCConnections.class.getName());
		
	private static ConcurrentHashMap<String,CProcFrameworkProtocol> DNtoRPC 
							= new ConcurrentHashMap<String,CProcFrameworkProtocol>();
	
	public static CProcFrameworkProtocol getRPC(String DN ,RpcConfiguration rpcConf){
		synchronized(DNtoRPC){
			CProcFrameworkProtocol CProcFrameworkNode = DNtoRPC.get(DN);	
			
			if(CProcFrameworkNode == null){
				try {
					CProcFrameworkNode = (CProcFrameworkProtocol)RPC.getProxy(CProcFrameworkProtocol.class,
									CProcFrameworkProtocol.versionID, 
									NetUtils.createSocketAddr(DN.split(":")[0] + ":" + CProcFramework.port), 
									rpcConf,NetUtils.getSocketFactory(rpcConf, CProcFrameworkProtocol.class));
				} catch (IOException e) {
					e.printStackTrace();
				}
//				finally{
//					//20131210修改RPC关闭
//					RPC.stopProxy(CProcFrameworkNode);
//				}
				DNtoRPC.put(DN, CProcFrameworkNode);
			}
			
			return CProcFrameworkNode;

		}
	}
	

	
}

