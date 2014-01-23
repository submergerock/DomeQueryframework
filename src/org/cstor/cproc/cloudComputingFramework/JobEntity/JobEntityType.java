package org.cstor.cproc.cloudComputingFramework.JobEntity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public enum JobEntityType {
					SUBMITED    ("submited"),
				    RUNNING   ("running"),
				    SUCCESS   ("success"),
				    WAITING   ("waiting"),
				    INDEX   ("indexing"),
				    SEARCH   ("searching"),
				    INNDIR   ("inndiring");
				    private String description = null;
				    private JobEntityType(String arg) {this.description = arg;}

				    public String toString() {
				      return description;
				    }
					public void readFields(DataInput in) throws IOException {
						this.description = in.readLine();
					}

					  public void write(DataOutput out) throws IOException {
						  out.writeChars(this.description);
					}
				  }

