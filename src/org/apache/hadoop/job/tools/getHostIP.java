package org.apache.hadoop.job.tools;

import java.net.*;
import java.util.*;

public class getHostIP {

	 public String getMacAddr() {
	String MacAddr = "";
	String str = "";
	try {
	NetworkInterface NIC = NetworkInterface.getByName("bond0");
	byte[] buf = NIC.getHardwareAddress();
	for (int i = 0; i < buf.length; i++) {
	str = str + byteHEX(buf[i]);
	}
	MacAddr = str.toUpperCase();
	} catch (SocketException e) {
	e.printStackTrace();
	System.exit(-1);
	}
	return MacAddr;
	}
	 public static String getLocalIP() {
			String ip = "";
			try {
			Enumeration<?> e1 = (Enumeration<?>) NetworkInterface
			.getNetworkInterfaces();
			while (e1.hasMoreElements()) {
			NetworkInterface ni = (NetworkInterface) e1.nextElement();
			if (!ni.getName().equals("eth0")) {
			continue;
			} else {
			Enumeration<?> e2 = ni.getInetAddresses();
			while (e2.hasMoreElements()) {
			InetAddress ia = (InetAddress) e2.nextElement();
			if (ia instanceof Inet6Address)
			continue; 
			ip = ia.getHostAddress();
			}
			break;
			}
			}
			} catch (SocketException e) {
			e.printStackTrace();
			System.exit(-1);
			}
			return ip;
			}
	 public static String getLocalIP(String network) {
	String ip = "";
	try {
	Enumeration<?> e1 = (Enumeration<?>) NetworkInterface
	.getNetworkInterfaces();
	while (e1.hasMoreElements()) {
		
	NetworkInterface ni = (NetworkInterface) e1.nextElement();
	System.out.print("ni.getName()" + ni.getName());
	if (!ni.getName().equals(network)) {
	continue;
	} else {
	Enumeration<?> e2 = ni.getInetAddresses();
	while (e2.hasMoreElements()) {
	InetAddress ia = (InetAddress) e2.nextElement();
	if (ia instanceof Inet6Address)
	continue; 
	ip = ia.getHostAddress();
	}
	break;
	}
	}
	} catch (SocketException e) {
	e.printStackTrace();
	System.exit(-1);
	}
	return ip;
	}

	public static String byteHEX(byte ib) {
	char[] Digit = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a',
	'b', 'c', 'd', 'e', 'f' };
	char[] ob = new char[2];
	ob[0] = Digit[(ib >>> 4) & 0X0F];
	ob[1] = Digit[ib & 0X0F];
	String s = new String(ob);
	return s;
	}
	

}
