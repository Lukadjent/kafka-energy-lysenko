import java.io.*;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.*;

/** Asynchronous producer + concurrent consumer, both measured with one monotonic clock. */
public class EnergyBenchmark {
    private static final ObjectMapper JSON = new ObjectMapper();

    static class Payload {
        final byte[] value;
        final String device;
        final String region;
        final String window;
        final String turbine;
        Payload(String line) throws Exception {
            value = line.getBytes(StandardCharsets.UTF_8);
            Map<?,?> item = JSON.readValue(line, Map.class);
            device = String.valueOf(item.get("device_id"));
            turbine = String.valueOf(item.get("turbine_type"));
            Map<?,?> location = (Map<?,?>) item.get("location");
            region = location == null ? "unknown" : String.format(Locale.ROOT, "%.1f:%.1f",
                ((Number)location.get("lat")).doubleValue(), ((Number)location.get("lon")).doubleValue());
            String stamp = String.valueOf(item.get("timestamp"));
            window = stamp.length() >= 16 ? stamp.substring(0,16) : stamp;
        }
    }

    static Properties properties(Map<String,Object> config, boolean producer) {
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getOrDefault("bootstrap", "127.0.0.1:9092"));
        props.put("client.id", (producer ? "producer" : "consumer") + "-lab1-lysenko");
        if (producer) {
            props.put("key.serializer", ByteArraySerializer.class.getName());
            props.put("value.serializer", ByteArraySerializer.class.getName());
            props.put("acks", "all");
            props.put("enable.idempotence", String.valueOf(config.getOrDefault("enable_idempotence", false)));
            props.put("retries", "3");
            props.put("batch.size", String.valueOf(config.getOrDefault("batch_size", 65536)));
            props.put("linger.ms", String.valueOf(config.getOrDefault("linger_ms", 10)));
            props.put("compression.type", String.valueOf(config.getOrDefault("compression", "none")));
            props.put("buffer.memory", String.valueOf(config.getOrDefault("buffer_memory", 33554432)));
            props.put("max.in.flight.requests.per.connection", String.valueOf(config.getOrDefault("max_in_flight", 5)));
            props.put("delivery.timeout.ms", "120000");
        } else {
            props.put("key.deserializer", ByteArrayDeserializer.class.getName());
            props.put("value.deserializer", ByteArrayDeserializer.class.getName());
            props.put("enable.auto.commit", "false");
            props.put("auto.offset.reset", "earliest");
            props.put("max.poll.records", "2000");
            props.put("fetch.min.bytes", "1");
            props.put("fetch.max.wait.ms", "100");
        }
        return props;
    }

    static double percentile(double[] sorted, double fraction) {
        return sorted[(int)Math.ceil(fraction * sorted.length) - 1];
    }

    static Map<String,Object> latencySummary(double[] values) {
        double[] sorted = values.clone();
        Arrays.sort(sorted);
        Map<String,Object> result = new LinkedHashMap<>();
        result.put("avg_ms", Arrays.stream(values).average().orElseThrow());
        result.put("p50_ms", percentile(sorted, .50));
        result.put("p95_ms", percentile(sorted, .95));
        result.put("p99_ms", percentile(sorted, .99));
        result.put("max_ms", sorted[sorted.length - 1]);
        return result;
    }

    static double metric(KafkaProducer<byte[],byte[]> producer, String name) {
        return producer.metrics().entrySet().stream()
            .filter(item -> item.getKey().name().equals(name) && item.getKey().group().equals("producer-metrics"))
            .mapToDouble(item -> ((Number)item.getValue().metricValue()).doubleValue()).findFirst().orElse(Double.NaN);
    }

    static long logBytes(Map<String,Object> config, String topic, int partitions) throws IOException {
        if (!config.containsKey("log_root")) return 0;
        long total = 0;
        for (int p=0; p<partitions; p++) {
            Path directory = Path.of(String.valueOf(config.get("log_root")), topic + "-" + p);
            if (Files.isDirectory(directory)) {
                try (var files = Files.list(directory)) {
                    for (Path file : files.filter(path -> path.toString().endsWith(".log")).toList()) total += Files.size(file);
                }
            }
        }
        return total;
    }

    public static void main(String[] args) throws Exception {
        Map<String,Object> config = JSON.readValue(Files.readAllBytes(Path.of(args[0])), Map.class);
        List<Payload> payloads = new ArrayList<>();
        for (String line : Files.readAllLines(Path.of(String.valueOf(config.get("payload_file"))), StandardCharsets.UTF_8)) {
            if (!line.isBlank()) payloads.add(new Payload(line));
        }
        if (payloads.isEmpty()) throw new IllegalArgumentException("Empty payload file");
        String topic = String.valueOf(config.get("topic"));
        int count = ((Number)config.getOrDefault("records", 10000)).intValue();
        int partitions = ((Number)config.getOrDefault("partitions", 3)).intValue();
        double rate = ((Number)config.getOrDefault("throughput", -1)).doubleValue();
        if (count <= 0 || partitions <= 0 || rate == 0 || rate < -1) throw new IllegalArgumentException("Invalid count, partitions or throughput");
        String strategy = String.valueOf(config.getOrDefault("strategy", "device"));
        double[] ackLatency = new double[count];
        double[] e2eLatency = new double[count];
        AtomicLongArray sentTimes = new AtomicLongArray(count);
        AtomicInteger errors = new AtomicInteger();
        AtomicReference<String> firstError = new AtomicReference<>();
        LongAdder[] distribution = new LongAdder[partitions];
        long[] scheduledBytes = new long[partitions];
        for (int p=0; p<partitions; p++) distribution[p] = new LongAdder();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (KafkaProducer<byte[],byte[]> producer = new KafkaProducer<>(properties(config, true))) {
            // Connection/JIT warm-up is excluded from the measured message counts.
            int warmup = ((Number)config.getOrDefault("warmup_records", 1000)).intValue();
            for (int i=0; i<warmup; i++) {
                Payload payload = payloads.get(i % payloads.size());
                producer.send(new ProducerRecord<>(topic, payload.device.getBytes(StandardCharsets.UTF_8), payload.value));
            }
            producer.flush();
            CountDownLatch ready = new CountDownLatch(1);
            Future<Integer> consumed = executor.submit(() -> {
                try (KafkaConsumer<byte[],byte[]> consumer = new KafkaConsumer<>(properties(config, false))) {
                    List<TopicPartition> assignment = new ArrayList<>();
                    for (int p=0; p<partitions; p++) assignment.add(new TopicPartition(topic,p));
                    consumer.assign(assignment);
                    Map<TopicPartition,Long> ends = consumer.endOffsets(assignment);
                    for (TopicPartition tp : assignment) consumer.seek(tp, ends.get(tp));
                    ready.countDown();
                    int received = 0;
                    BitSet seen = new BitSet(count);
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120 + (rate > 0 ? (long)(count / rate) : 0));
                    while (received < count && System.nanoTime() < deadline) {
                        ConsumerRecords<byte[],byte[]> records = consumer.poll(Duration.ofMillis(100));
                        for (ConsumerRecord<byte[],byte[]> record : records) {
                            Header header = record.headers().lastHeader("sequence");
                            if (header == null) throw new IllegalStateException("Unexpected record without sequence");
                            int sequence = ByteBuffer.wrap(header.value()).getInt();
                            if (sequence < 0 || sequence >= count || seen.get(sequence)) throw new IllegalStateException("Missing or duplicate sequence");
                            if (!Arrays.equals(record.value(), payloads.get(sequence % payloads.size()).value)) throw new IllegalStateException("Payload mismatch");
                            seen.set(sequence);
                            e2eLatency[sequence] = (System.nanoTime() - sentTimes.get(sequence)) / 1e6;
                            received++;
                        }
                    }
                    if (received != count) throw new IllegalStateException("Only received " + received + " of " + count);
                    return received;
                } finally { ready.countDown(); }
            });
            if (!ready.await(30, TimeUnit.SECONDS)) throw new TimeoutException("Consumer readiness timed out");
            if (consumed.isDone()) consumed.get();
            double outgoingBefore = metric(producer,"outgoing-byte-total");
            com.sun.management.OperatingSystemMXBean operatingSystem =
                (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            long cpuBefore = operatingSystem.getProcessCpuTime();
            long logBefore = logBytes(config, topic, partitions);
            System.out.println("MEASUREMENT_START " + topic);
            System.out.flush();
            long start = System.nanoTime();
            long payloadBytes = 0;
            for (int i=0; i<count; i++) {
                if (rate > 0) {
                    long wait = start + (long)(i * (1e9 / rate)) - System.nanoTime();
                    if (wait > 0) java.util.concurrent.locks.LockSupport.parkNanos(wait);
                }
                Payload payload = payloads.get(i % payloads.size());
                payloadBytes += payload.value.length;
                String key = strategy.equals("region") ? payload.region : strategy.equals("time") ? payload.window : strategy.equals("turbine") ? payload.turbine : payload.device;
                Integer explicitPartition = null;
                if (strategy.equals("balanced")) {
                    int least = 0;
                    for (int p=1; p<partitions; p++) if (scheduledBytes[p] < scheduledBytes[least]) least = p;
                    scheduledBytes[least] += payload.value.length;
                    explicitPartition = least;
                }
                ProducerRecord<byte[],byte[]> record = new ProducerRecord<>(topic, explicitPartition,
                    key.getBytes(StandardCharsets.UTF_8), payload.value);
                final int sequence = i;
                record.headers().add("sequence", ByteBuffer.allocate(4).putInt(i).array());
                sentTimes.set(i, System.nanoTime());
                producer.send(record, (metadata, error) -> {
                    if (error != null) { errors.incrementAndGet(); firstError.compareAndSet(null,error.toString()); }
                    else {
                        ackLatency[sequence] = (System.nanoTime() - sentTimes.get(sequence)) / 1e6;
                        distribution[metadata.partition()].increment();
                    }
                });
            }
            producer.flush();
            long ackEnd = System.nanoTime();
            int received = consumed.get(120,TimeUnit.SECONDS);
            long e2eEnd = System.nanoTime();
            if (errors.get() != 0) throw new IllegalStateException(firstError.get());
            Map<String,Object> output = new LinkedHashMap<>();
            output.put("config",config);
            output.put("records",count);
            output.put("received",received);
            output.put("errors",errors.get());
            output.put("payload_bytes",payloadBytes);
            output.put("broker_log_bytes",logBytes(config, topic, partitions)-logBefore);
            output.put("mean_payload_bytes",(double)payloadBytes/count);
            output.put("producer_seconds",(ackEnd-start)/1e9);
            output.put("end_to_end_seconds",(e2eEnd-start)/1e9);
            output.put("records_per_sec",count/((ackEnd-start)/1e9));
            output.put("ack_latency",latencySummary(ackLatency));
            output.put("e2e_latency",latencySummary(e2eLatency));
            output.put("jvm_cpu_ms",(operatingSystem.getProcessCpuTime()-cpuBefore)/1e6);
            output.put("cpu_percent_one_core",100.0*(operatingSystem.getProcessCpuTime()-cpuBefore)/(e2eEnd-start));
            output.put("producer_outgoing_bytes",metric(producer,"outgoing-byte-total")-outgoingBefore);
            output.put("compression_rate_avg",metric(producer,"compression-rate-avg"));
            output.put("batch_size_avg",metric(producer,"batch-size-avg"));
            output.put("records_per_request_avg",metric(producer,"records-per-request-avg"));
            output.put("bufferpool_wait_ratio",metric(producer,"bufferpool-wait-ratio"));
            output.put("partition_counts",Arrays.stream(distribution).mapToLong(LongAdder::sum).toArray());
            output.put("measurement_scope", Map.of(
                "timers_and_byte_deltas", "measured records only; warmup excluded",
                "producer_average_metrics", "same producer metric window; includes warmup when configured",
                "delivery", "normal operation only; no crash or retry fault injection"));
            Path destination = Path.of(String.valueOf(config.get("output_file")));
            JSON.writerWithDefaultPrettyPrinter().writeValue(destination.toFile(),output);
            // Emit the actual measurement at execution time, so every reported value
            // can be checked against the original key-aware performance-test stdout.
            System.out.println("MEASUREMENT_RESULT " + JSON.writeValueAsString(output));
            System.out.println("MEASUREMENT_DONE " + destination);
        } finally {
            executor.shutdownNow();
        }
    }
}
