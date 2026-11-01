package wm.fo;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import wm.common.kafka.Topics;
public class KafkaFOManager{
    private final String broker;
    private final String idFO;
    private KafkaProducer <String, String> producer;
    private KafkaConsumer <String, String> consumer;
    private volatile boolean running = true;

    public KafkaFOManager(String broker, String idFO){
        this.broker = broker;
        this.idFO= idFO;
        initProducer();
    }
    private void initProducer(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }
    
    public void iniciarEscucha(BiConsumer<String, String> procesadorMensajes){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "fo-group-" +idFO);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        //Grupo fijo: al reiniciarse, el FO recibe los mensajes que se perdió (p. ej. su RESUMEN).
        //Si el FO se cerró de golpe, Kafka tarda session.timeout.ms en liberar el grupo: 6 s (mínimo)
        //en vez de los 45 s por defecto
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000);

        this.consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList(
            Topics.RIEGO_PASOS_AUTORIZACION,
            Topics.ESTADO_RIEGO_FO,
            Topics.RESUMEN_RIEGO,
            Topics.FUGA_RIEGO,
            Topics.ESTADO_GLOBAL_WS
        ));
        Thread hilo = new Thread(() -> {
            try{
                while(running){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                    for(ConsumerRecord<String, String> record : records){
                        //Filtramos eventos globales o dirigidos a este FO
                        if(Topics.ESTADO_GLOBAL_WS.equals(record.topic()) || idFO.equals(record.key())){
                            procesadorMensajes.accept(record.topic(), record.value());
                        }
                    }
                }
            }catch(Exception e){
                if(running){
                    System.err.println("[KAFKA FO] Error en consumidor: " + e.getMessage());
                }
            } finally {
                if(consumer!=null){
                    consumer.close();
                }
            }
        }, "fo-kafka-listener-"+idFO);
        hilo.start();
    }
    public void solicitarRiego(String idWS, int duracionSegundos){
        //Formato del mensaje: idFO#idWS#duracion
        String mensaje = String.format("SOLICITUD#%s#%s#%d", idFO, idWS, duracionSegundos);
        ProducerRecord<String, String> record = new ProducerRecord<>(Topics.SOLICITAR_RIEGO, idWS, mensaje);
        producer.send(record);
    }
    public void detener(){
        running = false;
        if(producer !=null){
            producer.close();
        }
    }
}