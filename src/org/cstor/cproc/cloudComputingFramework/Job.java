package org.cstor.cproc.cloudComputingFramework;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Job implements Writable{
	private LongWritable jobId;
	private Text jobName;
	private Text date;
	private JobConfiguration conf;
	//private Writable message;	
	
	public Job(){		
		this(new JobConfiguration());
	}
	public Job(JobConfiguration conf){
		this(new LongWritable((long)((Math.random()*10000000)%100000)), 
				 new Text(""), 
				 new Text((new Date()).toString()), 
				 conf);
	}
	
    public Job(LongWritable jobId,Text jobName,Text date,JobConfiguration conf){
		this.jobId = jobId;
		this.jobName = jobName;
		this.date = date;
		this.conf = conf;
	}   
	
	public LongWritable getJobId() {
		return this.jobId;
	}

	public void setJobId(LongWritable jobId) {
		this.jobId = jobId;
	}

	public Text getJobName() {
		return this.jobName;
	}

	public void setJobName(Text jobName) {
		this.jobName = jobName;
	}

	public Text getDate() {
		return this.date;
	}

	public void setDate(Text date) {
		this.date = date;
	}

//	public Writable getMessage() {
//		return this.message;
//	}
//
//	public void setMessage(Writable message) {
//		this.message = message;
//	}	
	
	public synchronized JobConfiguration getConf() {
		return this.conf;
	}

	public void setConf(JobConfiguration conf) {
		this.conf = conf;
	}

	@Override
	public  void write(DataOutput out) throws IOException {
//		System.gc();
		
		synchronized (	this.conf) {
			
		
		this.conf.write(out);		
		this.date.write(out);
		this.jobId.write(out);
		this.jobName.write(out);
		}
		
		
		//this.message.write(out);
		//this.queryConditions.write(out);
		//out.writeInt(this.jobType.ordinal());		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.conf.readFields(in);
		this.date.readFields(in);
		this.jobId.readFields(in);
		this.jobName.readFields(in);	
		//this.message.readFields(in);
	}
}