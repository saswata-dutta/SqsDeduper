package org.saswata;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.publishers.cloudwatch.CloudWatchMetricPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.time.Duration;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) {

        final int NUM_THREADS = 20;
        final String queueUrl = "fixme";
        final String topicArn = "fixme";

        // create AWS clients
        final SdkHttpClient httpClient =
                ApacheHttpClient.builder()
                        .socketTimeout(Duration.ofMinutes(1))
                        .connectionTimeout(Duration.ofSeconds(30))
                        .maxConnections(Math.max(NUM_THREADS, 50))
                        .tcpKeepAlive(true)
                        .build();

        // TODO replace with default creds provider
        final AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
                "your_access_key_id",
                "your_secret_access_key");

        AwsCredentialsProvider credsProvider = StaticCredentialsProvider.create(awsCreds);

        final Region region = Region.EU_WEST_1;

        final MetricPublisher metricsPub = CloudWatchMetricPublisher.builder()
                .cloudWatchClient(CloudWatchAsyncClient.builder()
                        .region(region)
                        .credentialsProvider(credsProvider)
                        .build())
                .uploadFrequency(Duration.ofMinutes(5))
                .build();

        final SqsClient sqs = SqsClient.builder()
                .region(region)
                .credentialsProvider(credsProvider)
                .httpClient(httpClient)
                .defaultsMode(DefaultsMode.IN_REGION)
                .overrideConfiguration(c -> c.addMetricPublisher(metricsPub))
                .build();

        final SnsClient sns = SnsClient.builder()
                .region(region)
                .credentialsProvider(credsProvider)
                .httpClient(httpClient)
                .defaultsMode(DefaultsMode.IN_REGION)
                .overrideConfiguration(c -> c.addMetricPublisher(metricsPub))
                .build();


        // create Cache for deduplication
        final Cache<Long, Object> cache = Caffeine.newBuilder()
                .expireAfterWrite(120, TimeUnit.MINUTES)
                .maximumSize(1_000_000)
                .build();
        // get a view of the entries stored in cache as a thread-safe map.
        // Modifications made to the map directly affect the cache.
        final ConcurrentMap<Long, Object> dedup = cache.asMap();

        // Create Executor
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        for (int i = 0; i < NUM_THREADS; ++i) {
            executorService.submit(new SqsWorker(
                    sqs, queueUrl,
                    sns, topicArn,
                    dedup
            ));
        }

        System.out.println("Workers polling ...");
        // wait indefinitely
        while (true) {
            try {
                executorService.awaitTermination(100, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                // shouldn't happen
                e.printStackTrace();
                break;
            }
        }
    }
}
