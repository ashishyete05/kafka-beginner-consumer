package com.kafka.learning;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchConsumerForTwitter {

	final static Logger log = LoggerFactory.getLogger(ElasticSearchConsumerForTwitter.class);

	public static void main(String[] args) throws IOException {

		String jsonString = "{\"foo\":\"bar\"}";

		RestHighLevelClient client = createClient();
		//IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(jsonString, XContentType.JSON);
		//IndexResponse indexRespose = client.index(indexRequest, RequestOptions.DEFAULT);
		//String id = indexRespose.getId();
		//log.info(id);

		KafkaConsumer<String, String> kafkaCosumer = createKafkaConsumer("twitter_tweets");
		while (true) {
			
			ConsumerRecords<String, String> consumerRecords = kafkaCosumer.poll(Duration.ofMillis(1000));
			for(ConsumerRecord<String, String> consumerRecord : consumerRecords) {
				
				IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(consumerRecord.value(), XContentType.JSON);
				IndexResponse indexRespose = client.index(indexRequest, RequestOptions.DEFAULT);
				String id = indexRespose.getId();
				log.info(id);
				/*
				 * try { Thread.sleep(1000); } catch (InterruptedException e) { // TODO
				 * Auto-generated catch block e.printStackTrace(); }
				 */
			}
		}
		//client.close();
	}

	public static KafkaConsumer<String, String> createKafkaConsumer(String topic) {
		final String BOOTSTRAP_SERVERS = "localhost:9092";
		// final String TOPIC_NAME = "twitter_tweets";
		final String GROUP_NAME = "kafka-demo-elasticsearch";
		final String KEY_DESERIALIZER = StringDeserializer.class.getName();
		final String VALUE_DESERIALIZER = StringDeserializer.class.getName();

		// properties
		Properties properties = new Properties();
		properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER);
		properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER);
		properties.setProperty(GROUP_ID_CONFIG, GROUP_NAME);
		properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");

		// kafkaconsumer
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
		kafkaConsumer.subscribe(Arrays.asList(topic));

		return kafkaConsumer;
	}

	public static RestHighLevelClient createClient() {
//https://w6ezc37kzs:93nxvlmz0c@twitterfeed-testing-4831432889.us-east-1.bonsaisearch.net:443
		String hostname = "twitterfeed-testing-4831432889.us-east-1.bonsaisearch.net";
		String username = "w6ezc37kzs";
		String password = "93nxvlmz0c";

		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

		RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(hostname, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {

					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						// TODO Auto-generated method stub
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});

		RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);
		return client;
	}

}
