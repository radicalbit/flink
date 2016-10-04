package org.apache.flink.batch.connectors.elasticsearch;


import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;
import java.util.Map;

public interface ElasticsearchOutputFormatFunction<T> extends Serializable, Function {
	Map createSource(T element);
}
