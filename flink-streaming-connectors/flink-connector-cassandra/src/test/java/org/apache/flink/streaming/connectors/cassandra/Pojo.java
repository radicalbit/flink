package org.apache.flink.streaming.connectors.cassandra;

import java.io.Serializable;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace= "test", name = "mappersink")
public class Pojo implements Serializable {

	private static final long serialVersionUID = 1038054554690916991L;
	
	@Column(name = "id")
	private long id;
	@Column(name = "value")
	private String value;
	
	public Pojo(long id, String value){
		this.id = id;
		this.value = value;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
}