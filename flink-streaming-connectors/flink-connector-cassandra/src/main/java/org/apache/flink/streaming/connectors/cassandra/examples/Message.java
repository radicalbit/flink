package org.apache.flink.streaming.connectors.cassandra.examples;

import java.io.Serializable;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "test", name = "testtable")
public class Message implements Serializable {

	private static final long serialVersionUID = 1123119384361005680L;

	@Column(name = "word")
	private String message;

	public Message(String word) {
		this.message = word;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String word) {
		this.message = word;
	}

	public boolean equals(Object other) {
		if (other instanceof Message) {
			Message that = (Message) other;
			return this.message.equals(that.message);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return message.hashCode();
	}

}