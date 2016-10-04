package org.apache.flink.batch.connectors.elasticsearch;

import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.elasticsearch2.BulkProcessorIndexer;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ElasticsearchOutputFormat<T> extends RichOutputFormat<T> {

	public static final String CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS = "bulk.flush.max.actions";
	public static final String CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB = "bulk.flush.max.size.mb";
	public static final String CONFIG_KEY_BULK_FLUSH_INTERVAL_MS = "bulk.flush.interval.ms";

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchOutputFormat.class);

	/**
	 * The user specified config map that we forward to Elasticsearch when we create the Client.
	 */
	private final Properties userConfig;

	/**
	 * The list of nodes that the TransportClient should connect to. This is null if we are using
	 * an embedded Node to get a Client.
	 */
	private final List<InetSocketAddress> transportAddresses;

	/**
	 * The builder that is used to construct an {@link IndexRequest} from the incoming element.
	 */
	private final ElasticsearchOutputFormatFunction<T> elasticsearchIndexFunction;

	/**
	 * The Client that was either retrieved from a Node or is a TransportClient.
	 */
	private transient Client client;

	/**
	 * Bulk processor that was created using the client
	 */
	private transient BulkProcessor bulkProcessor;

	/**
	 * The index used by the {@link IndexRequest} created
	 */
	private final String index;

	/**
	 * The type used by the {@link IndexRequest} created
	 */
	private final String type;

	/**
	 * Bulk {@link org.elasticsearch.action.ActionRequest} indexer
	 */
	private transient RequestIndexer requestIndexer;

	/**
	 * This is set from inside the BulkProcessor listener if there where failures in processing.
	 */
	private final AtomicBoolean hasFailure = new AtomicBoolean(false);

	/**
	 * This is set from inside the BulkProcessor listener if a Throwable was thrown during processing.
	 */
	private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

	public ElasticsearchOutputFormat(Properties userConfig,
									 List<InetSocketAddress> transportAddresses,
									 String index,
									 String type,
									 ElasticsearchOutputFormatFunction<T> elasticsearchIndexFunction) {
		this.userConfig = userConfig;
		this.transportAddresses = transportAddresses;
		this.index = index;
		this.type = type;
		this.elasticsearchIndexFunction = elasticsearchIndexFunction;
	}

	@Override
	public void configure(Configuration parameters) {
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		List<TransportAddress> transportNodes;
		transportNodes = new ArrayList<>(transportAddresses.size());
		for (InetSocketAddress address : transportAddresses) {
			transportNodes.add(new InetSocketTransportAddress(address));
		}

		Settings settings = Settings.settingsBuilder().put(userConfig).build();

		TransportClient transportClient = TransportClient.builder().settings(settings).build();
		for (TransportAddress transport : transportNodes) {
			transportClient.addTransportAddress(transport);
		}

		ImmutableList<DiscoveryNode> nodes = ImmutableList.copyOf(transportClient.connectedNodes());
		if (nodes.isEmpty()) {
			throw new RuntimeException("Client is not connected to any Elasticsearch nodes!");
		}

		client = transportClient;

		if (LOG.isInfoEnabled()) {
			LOG.info("Created Elasticsearch TransportClient {}", client);
		}

		BulkProcessor.Builder bulkProcessorBuilder = BulkProcessor.builder(client, new BulkProcessor.Listener() {
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {

			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
				if (response.hasFailures()) {
					for (BulkItemResponse itemResp : response.getItems()) {
						if (itemResp.isFailed()) {
							LOG.error("Failed to index document in Elasticsearch: " + itemResp.getFailureMessage());
							failureThrowable.compareAndSet(null, new RuntimeException(itemResp.getFailureMessage()));
						}
					}
					hasFailure.set(true);
				}
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				LOG.error(failure.getMessage());
				failureThrowable.compareAndSet(null, failure);
				hasFailure.set(true);
			}
		});

		bulkProcessorBuilder.setConcurrentRequests(0);

		if (userConfig.containsKey(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS)) {
			bulkProcessorBuilder.setBulkActions(
				Integer.parseInt(userConfig.getProperty(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS)));
		}

		if (userConfig.containsKey(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB)) {
			bulkProcessorBuilder.setBulkSize(new ByteSizeValue(
				Integer.parseInt(userConfig.getProperty(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB)), ByteSizeUnit.MB));
		}

		if (userConfig.containsKey(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS)) {
			bulkProcessorBuilder.setFlushInterval(TimeValue.timeValueMillis(
				Integer.parseInt(userConfig.getProperty(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS))));
		}

		bulkProcessor = bulkProcessorBuilder.build();
		requestIndexer = new BulkProcessorIndexer(bulkProcessor);
	}

	@Override
	public void writeRecord(T record) throws IOException {
		requestIndexer.add(
			Requests.indexRequest(index).type(type)
				.source(elasticsearchIndexFunction.createSource(record)));
	}

	@Override
	public void close() throws IOException {
		if (bulkProcessor != null) {
			bulkProcessor.close();
			bulkProcessor = null;
		}

		if (client != null) {
			client.close();
		}

		if (hasFailure.get()) {
			Throwable cause = failureThrowable.get();
			if (cause != null) {
				throw new RuntimeException("An error occured in ElasticsearchSink.", cause);
			} else {
				throw new RuntimeException("An error occured in ElasticsearchSink.");
			}
		}
	}

}
