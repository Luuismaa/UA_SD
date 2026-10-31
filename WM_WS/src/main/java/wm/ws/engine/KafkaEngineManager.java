package wm.ws.engine;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import wm.common.kafka.Topics;
/**
 * Gestiona la conexión de la WS con Kafka
 * KafkaEngineManager
 */
public class KafkaEngineManager {
    private final String direccionKafka;
    private String idWS;
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;
    private volatile boolean running = true;
}
