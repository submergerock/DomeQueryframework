package org.apache.hadoop.job.tools;

import java.util.Date;

public class GCThread implements Runnable{

	@Override
	public void run() {
		// TODO Auto-generated method stub
		//2013.09.03增加GC线程
		while(true){
			boolean flag = false;
			Date date = new Date();
			System.out.println(date.getHours());
			if(date.getHours()==23){
				flag = true;
			}
			if(flag){
				System.gc();
				flag = false;
			}
			try {
				Thread.sleep(50*60*1000);//休眠50分钟
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	
	public static void main(String[] args) {
		Thread th = new Thread(new GCThread());
		th.start();
	}
}
