package pl.touk.nussknacker.engine.avro

import java.nio.charset.StandardCharsets

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.avro.Schema
import org.apache.flink.runtime.execution.ExecutionState
import org.apache.kafka.common.record.TimestampType
import pl.touk.nussknacker.engine.api.ProcessVersion
import pl.touk.nussknacker.engine.api.process.ProcessObjectDependencies
import pl.touk.nussknacker.engine.avro.KafkaAvroBaseTransformer.{SchemaVersionParamName, SinkKeyParamName, SinkValueParamName, TopicParamName, ValidationModeParameterName}
import pl.touk.nussknacker.engine.avro.encode.EncoderPolicy
import pl.touk.nussknacker.engine.avro.schema._
import pl.touk.nussknacker.engine.avro.schemaregistry.SchemaRegistryProvider
import pl.touk.nussknacker.engine.avro.schemaregistry.confluent.ConfluentSchemaRegistryProvider
import pl.touk.nussknacker.engine.avro.schemaregistry.confluent.client.{CachedConfluentSchemaRegistryClientFactory, ConfluentSchemaRegistryClientFactory, MockConfluentSchemaRegistryClientBuilder, MockSchemaRegistryClient}
import pl.touk.nussknacker.engine.build.EspProcessBuilder
import pl.touk.nussknacker.engine.kafka.KafkaConfig
import pl.touk.nussknacker.engine.process.compiler.FlinkProcessCompiler
import pl.touk.nussknacker.engine.process.{ExecutionConfigPreparer, FlinkStreamingProcessRegistrar}
import pl.touk.nussknacker.engine.spel
import pl.touk.nussknacker.engine.testing.LocalModelData
import pl.touk.nussknacker.engine.util.cache.DefaultCache

class KafkaAvroIntegrationSpec extends KafkaAvroSpecMixin {

  import KafkaAvroIntegrationMockSchemaRegistry._
  import pl.touk.nussknacker.engine.kafka.KafkaZookeeperUtils._
  import spel.Implicits._

  private lazy val creator: KafkaAvroTestProcessConfigCreator = new KafkaAvroTestProcessConfigCreator {
    override protected def createSchemaRegistryProvider(processObjectDependencies: ProcessObjectDependencies): SchemaRegistryProvider =
      ConfluentSchemaRegistryProvider(factory, processObjectDependencies)
  }

  protected val paymentSchemas: List[Schema] = List(PaymentV1.schema, PaymentV2.schema)
  protected val payment2Schemas: List[Schema] = List(PaymentV1.schema, PaymentV2.schema, PaymentNotCompatible.schema)

  override def schemaRegistryClient: MockSchemaRegistryClient = schemaRegistryMockClient

  override protected def confluentClientFactory: ConfluentSchemaRegistryClientFactory = factory

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    stoppableEnv.start()
    val modelData = LocalModelData(config, creator)
    registrar = FlinkStreamingProcessRegistrar(new FlinkProcessCompiler(modelData), config, ExecutionConfigPreparer.unOptimizedChain(modelData, None))
  }

  override protected def afterAll(): Unit = {
    stoppableEnv.stop()
    super.afterAll()
  }

  test("should read event in the same version as source requires and save it in the same version") {
    val topicConfig = createAndRegisterTopicConfig("simple", PaymentV1.schema)
    val sourceParam = SourceAvroParam.forGeneric(topicConfig, Some(1))
    val sinkParam = SinkAvroParam(topicConfig, Some(1), "#input")
    val process = createAvroProcess(sourceParam, sinkParam)

    runAndVerifyResult(process, topicConfig, PaymentV1.record, PaymentV1.record)
  }

  test("should read primitive event and save it in the same format") {
    val topicConfig = createAndRegisterTopicConfig("simple.primitive", Schema.create(Schema.Type.STRING))
    val sourceParam = SourceAvroParam.forGeneric(topicConfig, None)
    val sinkParam = SinkAvroParam(topicConfig, None, "#input")
    val process = createAvroProcess(sourceParam, sinkParam)

    runAndVerifyResult(process, topicConfig, "fooBar", "fooBar")
  }

  test("should read newer compatible event then source requires and save it in older compatible version") {
    val topicConfig = createAndRegisterTopicConfig("newer-older-older", paymentSchemas)
    val sourceParam = SourceAvroParam.forGeneric(topicConfig, Some(1))
    val sinkParam = SinkAvroParam(topicConfig, Some(1), "#input")
    val process = createAvroProcess(sourceParam, sinkParam)

    runAndVerifyResult(process, topicConfig, PaymentV2.record, PaymentV1.record)
  }

  test("should read older compatible event then source requires and save it in newer compatible version") {
    val topicConfig = createAndRegisterTopicConfig("older-newer-newer", paymentSchemas)
    val sourceParam = SourceAvroParam.forGeneric(topicConfig, Some(2))
    val sinkParam = SinkAvroParam(topicConfig, Some(2), "#input")
    val process = createAvroProcess(sourceParam, sinkParam)

    runAndVerifyResult(process, topicConfig, PaymentV1.record, PaymentV2.record)
  }

  test("should read event in the same version as source requires and save it in newer compatible version") {
    val topicConfig = createAndRegisterTopicConfig("older-older-newer", paymentSchemas)
    val sourceParam = SourceAvroParam.forGeneric(topicConfig, Some(1))
    val sinkParam = SinkAvroParam(topicConfig, Some(2), "#input", validationMode = EncoderPolicy.allowOptional)
    val process = createAvroProcess(sourceParam, sinkParam)

    runAndVerifyResult(process, topicConfig, PaymentV1.record, PaymentV2.record)
  }

  test("should read older compatible event then source requires and save it in older compatible version") {
    val topicConfig = createAndRegisterTopicConfig("older-newer-older", paymentSchemas)
    val sourceParam = SourceAvroParam.forGeneric(topicConfig, Some(2))
    val sinkParam = SinkAvroParam(topicConfig, Some(1), "#input", validationMode = EncoderPolicy.allowRedundantAndOptional)
    val process = createAvroProcess(sourceParam, sinkParam)

    runAndVerifyResult(process, topicConfig, PaymentV1.record, PaymentV1.record)
  }

  test("should read older compatible event with source and save it in latest compatible version") {
    val topicConfig = createAndRegisterTopicConfig("older-latest-latest", paymentSchemas)
    val sourceParam = SourceAvroParam.forGeneric(topicConfig, None)
    val sinkParam = SinkAvroParam(topicConfig, None, "#input")
    val process = createAvroProcess(sourceParam, sinkParam)

    runAndVerifyResult(process, topicConfig, PaymentV1.record, PaymentV2.record)
  }

  test("should read older compatible event then source requires, filter and save it in older compatible version") {
    val topicConfig = createAndRegisterTopicConfig("older-newer-filter-older", paymentSchemas)
    val sourceParam = SourceAvroParam.forGeneric(topicConfig, Some(2))
    val sinkParam = SinkAvroParam(topicConfig, Some(1), "#input", validationMode = EncoderPolicy.allowRedundantAndOptional)
    val filerParam = Some("#input.cnt == 0")
    val process = createAvroProcess(sourceParam, sinkParam, filerParam)

    runAndVerifyResult(process, topicConfig, PaymentV1.record, PaymentV1.record)
  }

  test("should read compatible events with source, filter and save only one") {
    val topicConfig = createAndRegisterTopicConfig("two-source-filter-one", paymentSchemas)
    val sourceParam = SourceAvroParam.forGeneric(topicConfig, Some(2))
    val sinkParam = SinkAvroParam(topicConfig, Some(2), "#input")
    val filerParam = Some("#input.cnt == 1")
    val process = createAvroProcess(sourceParam, sinkParam, filerParam)
    val events = List(PaymentV1.record, PaymentV2.recordWithData)

    runAndVerifyResult(process, topicConfig, events, PaymentV2.recordWithData)
  }

  test("should read newer (back-compatible) newer event with source and save it in older compatible version") {
    val topicConfig = createAndRegisterTopicConfig("bc-older-older", payment2Schemas)
    val sourceParam = SourceAvroParam.forGeneric(topicConfig, Some(2))
    val sinkParam = SinkAvroParam(topicConfig, Some(1), "#input", validationMode = EncoderPolicy.allowRedundantAndOptional)
    val process = createAvroProcess(sourceParam, sinkParam)

    runAndVerifyResult(process, topicConfig, PaymentNotCompatible.record, PaymentV1.record)
  }

  test("should read older compatible event with source and save it in latest compatible version with map output") {
    val topicConfig = createAndRegisterTopicConfig("older-output-with-map", List(PaymentV1.schema, PaymentV2.schema))
    val sourceParam = SourceAvroParam.forGeneric(topicConfig, Some(1))
    val sinkParam = SinkAvroParam(topicConfig, None, PaymentV2.jsonMap)
    val process = createAvroProcess(sourceParam, sinkParam)

    runAndVerifyResult(process, topicConfig, PaymentV1.record, PaymentV2.record)
  }

  test("should read newer compatible event with source and save it in older compatible version with map output") {
    val topicConfig = createAndRegisterTopicConfig("newer-output-with-map", List(PaymentV1.schema, PaymentV2.schema))
    val sourceParam = SourceAvroParam.forGeneric(topicConfig, Some(2))
    val sinkParam = SinkAvroParam(topicConfig, Some(1), PaymentV1.jsonMap)
    val process = createAvroProcess(sourceParam, sinkParam)

    runAndVerifyResult(process, topicConfig, PaymentV2.record, PaymentV1.record)
  }

  test("should rise exception when we provide wrong data map for #Avro helper output") {
    val topicConfig = createAndRegisterTopicConfig("bad-data-with-helper", List(PaymentV1.schema, PaymentV2.schema))
    val sourceParam = SourceAvroParam.forGeneric(topicConfig, Some(2))
    val sinkParam = SinkAvroParam(topicConfig, Some(1), """{id: "bad"}""")
    val process = createAvroProcess(sourceParam, sinkParam)

    assertThrowsWithParent[Exception] {
      runAndVerifyResult(process, topicConfig, PaymentV2.record, PaymentV1.record)
    }
  }

  test("should throw exception when try to filter by missing field") {
    val topicConfig = createAndRegisterTopicConfig("try-filter-by-missing-field", paymentSchemas)
    val sourceParam = SourceAvroParam.forGeneric(topicConfig, Some(1))
    val sinkParam = SinkAvroParam(topicConfig, Some(1), "#input")
    val filerParam = Some("#input.cnt == 1")
    val events = List(PaymentV1.record, PaymentV2.record)
    val process = createAvroProcess(sourceParam, sinkParam, filerParam)

    assertThrowsWithParent[Exception] {
      runAndVerifyResult(process, topicConfig, events, PaymentV2.recordWithData)
    }
  }

  test("should throw exception when try to convert not compatible event") {
    val topicConfig = createAndRegisterTopicConfig("try-to-convert-not-compatible", payment2Schemas)
    val sourceParam = SourceAvroParam.forGeneric(topicConfig, Some(3))
    val sinkParam = SinkAvroParam(topicConfig, Some(3), "#input")
    val process = createAvroProcess(sourceParam, sinkParam)

    /**
     * When we try deserialize not compatible event then exception will be thrown..
     * After that flink will stopped working.. And we can't find job. It can take some time.
     */
    pushMessage(PaymentV2.recordWithData, topicConfig.input)
    registrar.register(env, process, ProcessVersion.empty)
    val executionResult = stoppableEnv.executeAndWaitForStart(process.id)
    stoppableEnv.waitForJobState(executionResult.getJobID, process.id, ExecutionState.FAILED, ExecutionState.CANCELED)()
    stoppableEnv.cleanupGraph()
  }

  test("should pass timestamp from flink to kafka") {
    val topicConfig = createAndRegisterTopicConfig("timestamp-flink-kafka", LongFieldV1.schema)
    //Can't be too long ago, otherwise retention could delete it
    val timeToSetInProcess = System.currentTimeMillis() - 600000L

    val process = EspProcessBuilder
      .id("avro-test-timestamp-flink-kafka").parallelism(1).exceptionHandler()
      .source(
        "start", "kafka-avro", TopicParamName -> s"'${topicConfig.input}'", SchemaVersionParamName -> ""
      ).customNode("transform", "extractedTimestamp", "extractAndTransformTimestmp",
      "timestampToSet" -> (timeToSetInProcess.toString + "L"))
      .emptySink(
        "end",
        "kafka-avro",
        TopicParamName -> s"'${topicConfig.output}'",
        SchemaVersionParamName -> "",
        SinkKeyParamName -> "",
        ValidationModeParameterName -> s"'${EncoderPolicy.strict.name}'",
        SinkValueParamName -> s"{field: #extractedTimestamp}"
      )

    pushMessage(LongFieldV1.record, topicConfig.input)
    kafkaClient.createTopic(topicConfig.output)
    run(process) {
      val consumer = kafkaClient.createConsumer()
      val message = consumer.consumeWithConsumerRecord(topicConfig.output).head
      message.timestamp() shouldBe timeToSetInProcess
      message.timestampType() shouldBe TimestampType.CREATE_TIME
    }
  }

  test("should pass timestamp from kafka to flink") {
    val topicConfig = createAndRegisterTopicConfig("timestamp-kafka-flink", LongFieldV1.schema)

    val process = EspProcessBuilder
      .id("avro-test-timestamp-kafka-flink").parallelism(1).exceptionHandler()
      .source(
        "start", "kafka-avro", TopicParamName -> s"'${topicConfig.input}'", SchemaVersionParamName -> ""
      ).customNode("transform", "extractedTimestamp", "extractAndTransformTimestmp",
      "timestampToSet" -> "10000")
      .emptySink(
        "end",
        "kafka-avro",
        TopicParamName -> s"'${topicConfig.output}'",
        SchemaVersionParamName -> "",
        SinkKeyParamName -> "",
        ValidationModeParameterName -> s"'${EncoderPolicy.strict.name}'",
        SinkValueParamName -> s"{field: #extractedTimestamp}"
      )

    //Can't be too long ago, otherwise retention could delete it
    val timePassedThroughKafka = System.currentTimeMillis() - 120000L
    pushMessage(LongFieldV1.encodeData(-1000L), topicConfig.input, timestamp = timePassedThroughKafka)
    kafkaClient.createTopic(topicConfig.output)
    run(process) {
      consumeAndVerifyMessages(topicConfig.output, List(LongFieldV1.encodeData(timePassedThroughKafka)))
    }
  }

  test("should accept logical types in generic record") {
    val topicConfig = createAndRegisterTopicConfig("logical-fields-generic", List(PaymentDate.schema))
    val sourceParam = SourceAvroParam.forGeneric(topicConfig, None)
    val sinkParam = SinkAvroParam(topicConfig, None, "#input")
    val process = createAvroProcess(sourceParam, sinkParam, Some(
      s"#input.dateTime.toEpochMilli == ${PaymentDate.instant.toEpochMilli}L AND " +
        s"#input.date.year == ${PaymentDate.date.getYear} AND #input.date.monthValue == ${PaymentDate.date.getMonthValue} AND #input.date.dayOfMonth == ${PaymentDate.date.getDayOfMonth} AND " +
        s"#input.time.hour == ${PaymentDate.date.getHour} AND #input.time.minute == ${PaymentDate.date.getMinute} AND #input.time.second == ${PaymentDate.date.getSecond} AND " +
        s"#input.decimal == ${PaymentDate.decimal} AND " +
        s"#input.uuid.mostSignificantBits == ${PaymentDate.uuid.getMostSignificantBits}L AND #input.uuid.leastSignificantBits == ${PaymentDate.uuid.getLeastSignificantBits}L"))

    runAndVerifyResult(process, topicConfig, PaymentDate.recordWithData, PaymentDate.record)
  }

  test("should accept logical types in specific record") {
    val topicConfig = createAndRegisterTopicConfig("logical-fields-specific", List(
      GeneratedAvroClassWithLogicalTypesOldSchema.schema,
      GeneratedAvroClassWithLogicalTypes.SCHEMA$,
      GeneratedAvroClassWithLogicalTypesNewSchema.schema
    ))
    val sourceParam = SourceAvroParam.forSpecific(topicConfig)
    val sinkParam = SinkAvroParam(topicConfig, Some(2), "#input")

    val givenRecord = GeneratedAvroClassWithLogicalTypesOldSchema(
      PaymentDate.instant,
      PaymentDate.date.toLocalDate,
      PaymentDate.date.toLocalTime,
      java.math.BigDecimal.valueOf(PaymentDate.decimal))

    val expectedRecord = GeneratedAvroClassWithLogicalTypes.newBuilder()
      .setDateTime(PaymentDate.instant)
      .setDate(PaymentDate.date.toLocalDate)
      .setTime(PaymentDate.date.toLocalTime)
      .setDecimal(java.math.BigDecimal.valueOf(PaymentDate.decimal))
      .build()

    val process = createAvroProcess(sourceParam, sinkParam, Some(
      s"#input.dateTime.toEpochMilli == ${PaymentDate.instant.toEpochMilli}L AND " +
        s"#input.date.year == ${PaymentDate.date.getYear} AND #input.date.monthValue == ${PaymentDate.date.getMonthValue} AND #input.date.dayOfMonth == ${PaymentDate.date.getDayOfMonth} AND " +
        s"#input.time.hour == ${PaymentDate.date.getHour} AND #input.time.minute == ${PaymentDate.date.getMinute} AND #input.time.second == ${PaymentDate.date.getSecond} AND " +
        s"#input.decimal == ${PaymentDate.decimal} AND" +
        s"#input.text.toString == '123'")) // default value

    runAndVerifyResult(process, topicConfig, givenRecord, expectedRecord, useSpecificAvroReader = true)
  }

  test("should define kafka key for output record") {
    val topicConfig = createAndRegisterTopicConfig("kafka-key", List(FullNameV1.schema))
    val sourceParam = SourceAvroParam.forGeneric(topicConfig, None)
    val sinkParam = SinkAvroParam(topicConfig, None, value = "#input", key = "#input.first")
    val process = createAvroProcess(sourceParam, sinkParam, None)

    kafkaClient.createTopic(topicConfig.input, partitions = 1)
    pushMessage(FullNameV1.record, topicConfig.input)
    kafkaClient.createTopic(topicConfig.output, partitions = 1)

    run(process) {
      val consumer = kafkaClient.createConsumer()
      val consumed = consumer.consume(topicConfig.output, 20).take(1).head
      val consumedKey = new String(consumed.key(), StandardCharsets.UTF_8)
      consumedKey shouldEqual FullNameV1.BaseFirst
    }
  }

  test("should use null key for empty key expression") {
    val topicConfig = createAndRegisterTopicConfig("kafka-null-key", List(FullNameV1.schema))
    val sourceParam = SourceAvroParam.forGeneric(topicConfig, None)
    val sinkParam = SinkAvroParam(topicConfig, None, value = "#input")
    val process = createAvroProcess(sourceParam, sinkParam, None)

    kafkaClient.createTopic(topicConfig.input, partitions = 1)
    pushMessage(FullNameV1.record, topicConfig.input)
    kafkaClient.createTopic(topicConfig.output, partitions = 1)

    run(process) {
      val consumer = kafkaClient.createConsumer()
      val consumed = consumer.consume(topicConfig.output, 20).take(1).head
      consumed.key() shouldBe null
    }
  }

}

object KafkaAvroIntegrationMockSchemaRegistry {

  val schemaRegistryMockClient: MockSchemaRegistryClient =
    new MockConfluentSchemaRegistryClientBuilder()
      .build

  /**
   * It has to be done in this way, because schemaRegistryMockClient is not serializable..
   * And when we use TestSchemaRegistryClientFactory then flink has problem with serialization this..
   */
  val factory: CachedConfluentSchemaRegistryClientFactory =
    new CachedConfluentSchemaRegistryClientFactory(DefaultCache.defaultMaximumSize, None, None, None) {
      override protected def confluentClient(kafkaConfig: KafkaConfig): SchemaRegistryClient =
        schemaRegistryMockClient
    }
}
