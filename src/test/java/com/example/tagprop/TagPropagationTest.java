package com.example.tagprop;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class TagPropagationTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final DateTimeFormatter SQL_TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
      .withZone(ZoneOffset.UTC);

  @Container
  static final KafkaContainer KAFKA = new KafkaContainer(
      DockerImageName.parse("confluentinc/cp-kafka:7.5.3")
  );

  private JobClient jobClient;

  @AfterEach
  void stopJob() throws Exception {
    if (jobClient != null) {
      jobClient.cancel().get();
      jobClient = null;
    }
  }

  @Test
  @Timeout(180)
  void fixedPointDownstreamTagPropagation_addAndRemoveTags() throws Exception {
    String bootstrap = KAFKA.getBootstrapServers();

    createTopics(bootstrap, List.of(
        "assets",
        "relationships",
        "asset_tags_direct",
        "downstream",
        "asset_tags_materialized"
    ));

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(1000);

    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

    Configuration cfg = tableEnv.getConfig().getConfiguration();
    cfg.setString("execution.checkpointing.interval", "1s");
    cfg.setString("table.exec.source.idle-timeout", "5s");

    String rawSql = readResource("flink.sql");
    List<String> statements = splitSqlStatements(rawSql);

    for (String stmt : statements) {
      if (stmt.trim().isEmpty()) {
        continue;
      }
      if (stmt.trim().toUpperCase().startsWith("CREATE TABLE")) {
        stmt = addKafkaOptions(stmt, bootstrap);
      }
      TableResult result = tableEnv.executeSql(stmt);
      if (stmt.trim().toUpperCase().startsWith("EXECUTE STATEMENT SET")) {
        jobClient = result.getJobClient().orElseThrow();
      }
    }

    try (KafkaProducer<String, String> producer = newProducer(bootstrap)) {
      String t0 = sqlTimestampNow();

      // relationships A->B, B->C
      sendUpsert(producer, "relationships",
          json(Map.of("parent_id", "A", "child_id", "B")),
          json(Map.of("parent_id", "A", "child_id", "B", "event_time", t0)));
      sendUpsert(producer, "relationships",
          json(Map.of("parent_id", "B", "child_id", "C")),
          json(Map.of("parent_id", "B", "child_id", "C", "event_time", t0)));

      // direct tag on A
      sendUpsert(producer, "asset_tags_direct",
          json(Map.of("asset_id", "A", "tag", "T1")),
          json(Map.of("asset_id", "A", "tag", "T1", "event_time", t0)));
    }

    Map<String, Set<String>> expected = new HashMap<>();
    expected.put("A", Set.of("T1"));
    expected.put("B", Set.of("T1"));
    expected.put("C", Set.of("T1"));

    try (KafkaConsumer<String, String> consumer = newConsumer(bootstrap, "asset_tags_materialized")) {
      Map<String, Set<String>> state = awaitTags(consumer, expected, Duration.ofSeconds(60));
      assertEquals(expected, state);

      try (KafkaProducer<String, String> producer = newProducer(bootstrap)) {
        sendDelete(producer, "asset_tags_direct", json(Map.of("asset_id", "A", "tag", "T1")));
      }

      Map<String, Set<String>> afterDelete = awaitTags(consumer, Map.of(), Duration.ofSeconds(60));
      assertEquals(Map.of(), afterDelete);
    }
  }

  private static void createTopics(String bootstrap, List<String> topics) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrap);
    try (AdminClient admin = AdminClient.create(props)) {
      List<NewTopic> newTopics = new ArrayList<>();
      for (String t : topics) {
        newTopics.add(new NewTopic(t, 1, (short) 1));
      }
      admin.createTopics(newTopics).all().get();
    }
  }

  private static KafkaProducer<String, String> newProducer(String bootstrap) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    return new KafkaProducer<>(props);
  }

  private static KafkaConsumer<String, String> newConsumer(String bootstrap, String topic) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "tag-prop-test-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(List.of(topic));
    return consumer;
  }

  private static void sendUpsert(KafkaProducer<String, String> producer, String topic, String keyJson, String valueJson) {
    producer.send(new ProducerRecord<>(topic, keyJson, valueJson));
    producer.flush();
  }

  private static void sendDelete(KafkaProducer<String, String> producer, String topic, String keyJson) {
    producer.send(new ProducerRecord<>(topic, keyJson, null));
    producer.flush();
  }

  private static Map<String, Set<String>> awaitTags(KafkaConsumer<String, String> consumer,
                                                    Map<String, Set<String>> expected,
                                                    Duration timeout) throws Exception {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    Map<String, Set<String>> state = new HashMap<>();
    while (System.currentTimeMillis() < deadline) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
      for (ConsumerRecord<String, String> record : records) {
        String assetId = parseAssetId(record.key());
        if (record.value() == null) {
          state.remove(assetId);
          continue;
        }
        Set<String> tags = parseTags(record.value());
        if (tags.isEmpty()) {
          state.remove(assetId);
        } else {
          state.put(assetId, tags);
        }
      }
      if (stateEquals(state, expected)) {
        return new HashMap<>(state);
      }
      Thread.sleep(100);
    }
    throw new AssertionError("Timed out waiting for tags. Expected=" + expected + " Actual=" + state);
  }

  private static boolean stateEquals(Map<String, Set<String>> actual, Map<String, Set<String>> expected) {
    if (actual.size() != expected.size()) {
      return false;
    }
    for (Map.Entry<String, Set<String>> e : expected.entrySet()) {
      Set<String> actualTags = actual.get(e.getKey());
      if (actualTags == null) {
        return false;
      }
      if (!actualTags.equals(e.getValue())) {
        return false;
      }
    }
    return true;
  }

  private static String parseAssetId(String keyJson) throws IOException {
    JsonNode key = MAPPER.readTree(keyJson);
    return key.get("asset_id").asText();
  }

  private static Set<String> parseTags(String valueJson) throws IOException {
    JsonNode node = MAPPER.readTree(valueJson);
    JsonNode tagsNode = node.get("tags");
    Set<String> tags = new HashSet<>();
    if (tagsNode != null && tagsNode.isArray()) {
      for (JsonNode t : tagsNode) {
        if (t.isTextual()) {
          tags.add(t.asText());
        } else if (t.isObject()) {
          JsonNode element = t.get("element");
          JsonNode count = t.get("count");
          if (element != null && (count == null || count.asInt() > 0)) {
            tags.add(element.asText());
          }
        }
      }
    } else if (tagsNode != null && tagsNode.isObject()) {
      tagsNode.fields().forEachRemaining(e -> {
        JsonNode count = e.getValue();
        if (count == null || count.asInt() > 0) {
          tags.add(e.getKey());
        }
      });
    }
    return tags;
  }

  private static String json(Map<String, Object> data) throws IOException {
    return MAPPER.writeValueAsString(data);
  }

  private static String sqlTimestampNow() {
    return SQL_TS.format(Instant.now());
  }

  private static String readResource(String name) throws IOException {
    try (InputStream in = TagPropagationTest.class.getClassLoader().getResourceAsStream(name)) {
      if (in == null) {
        throw new IllegalStateException("Missing resource: " + name);
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private static List<String> splitSqlStatements(String sql) {
    int idx = sql.toUpperCase().indexOf("EXECUTE STATEMENT SET");
    String head = idx >= 0 ? sql.substring(0, idx) : sql;
    String tail = idx >= 0 ? sql.substring(idx) : "";

    List<String> result = new ArrayList<>();
    for (String part : head.split(";")) {
      String trimmed = stripSqlComments(part).trim();
      if (!trimmed.isEmpty()) {
        result.add(trimmed);
      }
    }
    if (!tail.trim().isEmpty()) {
      result.add(tail.trim());
    }
    return result;
  }

  private static String stripSqlComments(String sql) {
    StringBuilder out = new StringBuilder();
    String[] lines = sql.split("\\R");
    for (String line : lines) {
      String trimmed = line.trim();
      if (trimmed.startsWith("--")) {
        continue;
      }
      int idx = line.indexOf("--");
      if (idx >= 0) {
        out.append(line, 0, idx);
      } else {
        out.append(line);
      }
      out.append('\n');
    }
    return out.toString();
  }

  private static String addKafkaOptions(String createTableSql, String bootstrap) {
    String upper = createTableSql.toUpperCase();
    if (!upper.contains("'CONNECTOR' = 'UPSERT-KAFKA'")) {
      return createTableSql;
    }

    int withIdx = upper.lastIndexOf("WITH");
    int openIdx = createTableSql.indexOf('(', withIdx);
    int closeIdx = createTableSql.lastIndexOf(')');
    if (withIdx < 0 || openIdx < 0 || closeIdx < 0 || closeIdx <= openIdx) {
      return createTableSql;
    }

    String options = createTableSql.substring(openIdx + 1, closeIdx);
    if (options.toLowerCase().contains("properties.bootstrap.servers")) {
      return createTableSql;
    }

    StringBuilder newOptions = new StringBuilder(options.trim());
    if (newOptions.length() > 0 && newOptions.charAt(newOptions.length() - 1) != ',') {
      newOptions.append(',');
    }
    newOptions.append("\n  'properties.bootstrap.servers' = '").append(bootstrap).append("',")
        .append("\n  'properties.group.id' = 'tag-prop-test'");

    return createTableSql.substring(0, openIdx + 1)
        + "\n  " + newOptions + "\n"
        + createTableSql.substring(closeIdx);
  }
}
