import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.errors.*;
import io.minio.messages.Bucket;
import io.minio.messages.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.List;

public class Minio {

    protected static final Logger LOGGER = LoggerFactory.getLogger(Minio.class);
    static final int MINIO_DEFAULT_PORT = 9000;
    static final String DEFAULT_IMAGE = "minio/minio";
    static final String DEFAULT_TAG = "latest";
    static final String DEFAULT_STORAGE_DIRECTORY = "/data";
    static final String HEALTH_ENDPOINT = "/minio/health/ready";
    static final String TEST_BUCKET = "testbucket";
    static final String MINIO_ACCESS_KEY = "minio";
    static final String MINIO_SECRET_KEY = "minio123";
    public GenericContainer<?> container;
    public MinioClient client;

    public Minio() throws Exception {
        this.container = new GenericContainer<>(DEFAULT_IMAGE + ':' + DEFAULT_TAG)
                .waitingFor(new HttpWaitStrategy()
                        .forPath(HEALTH_ENDPOINT)
                        .forPort(MINIO_DEFAULT_PORT)
                        .withStartupTimeout(Duration.ofSeconds(30)))
                .withEnv("MINIO_ACCESS_KEY", MINIO_ACCESS_KEY)
                .withEnv("MINIO_SECRET_KEY", MINIO_SECRET_KEY)
                .withExposedPorts(MINIO_DEFAULT_PORT)
                .withCommand("server " + DEFAULT_STORAGE_DIRECTORY);
    }

    public void start() throws Exception {

        this.container.start();
        this.container.waitingFor(Wait.forHttp("/minio/health/ready"));

        client = MinioClient.builder()
                .endpoint("http://" + container.getHost() + ":" + container.getMappedPort(MINIO_DEFAULT_PORT))
                .credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
                .build();
        client.ignoreCertCheck();

        client.makeBucket(MakeBucketArgs.builder()
                .bucket(TEST_BUCKET)
                .build());

        LOGGER.info("Minio Started!");
    }

    public void stop() {
        try {
            if (container != null) {
                container.stop();
            }
        }
        catch (Exception e) {
            // ignored
        }
    }

    public Integer getMappedPort() {
        return this.container.getMappedPort(MINIO_DEFAULT_PORT);
    }

    public void listBuckets()
            throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException,
            XmlParserException, ErrorResponseException {
        List<Bucket> bucketList = client.listBuckets();
        for (Bucket bucket : bucketList) {
            LOGGER.info("Bucket: {} {}", bucket.name(), bucket.creationDate());
        }
    }

    public void listFiles() {
        listFiles(null);
    }

    public void listFiles(String message) {
        LOGGER.info("-----------------------------------------------------------------");
        if (message != null) {
            LOGGER.info("{}", message);
        }
        try {
            List<Bucket> bucketList = client.listBuckets();
            for (Bucket bucket : bucketList) {
                LOGGER.info("Bucket:{} ROOT", bucket.name());
                Iterable<Result<Item>> results =
                        client.listObjects(ListObjectsArgs.builder().bucket(bucket.name()).recursive(true).build());
                for (Result<Item> result : results) {
                    Item item = result.get();
                    LOGGER.info("Bucket:{} Item:{} Size:{}", bucket.name(), item.objectName(), item.size());
                }
            }
        }
        catch (Exception e) {
            LOGGER.info("Failed listing bucket");
        }
        LOGGER.info("-----------------------------------------------------------------");

    }

}
