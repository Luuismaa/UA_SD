package wm.ws.engine;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.BiConsumer;

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
    
    public KafkaEngineManager(String direccionKafka){
        this.direccionKafka = direccionKafka;
        initProducer();
    }
    public void iniciarConsumidor(String idWS, BiConsumer<String, String> procesadorMensajes){
        this.idWS = idWS;
        initConsumer();
        Thread hiloConsumer = new Thread (()-> bucleConsumidor(procesadorMensajes), "kafka-consumer-" + idWS);
        hiloConsumer.start();
    }
    private void initProducer(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, direccionKafka);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }
    private void initConsumer(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, direccionKafka);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "engine-group-" + idWS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //empezar a leer desde los ultimos mensajes recibidos
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        this.consumer =new KafkaConsumer<>(props);
    }
    private void bucleConsumidor(BiConsumer<String, String> procesadorMensajes){
        consumer.subscribe(Collections.singletonList(Topics.ORDENES_RIEGO));
        System.out.println("[KAFKA] Escuchando órdenes en topic: " + Topics.ORDENES_RIEGO);
        try{
            while(running){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for(ConsumerRecord<String, String> record: records){
                    //Acepta ordenes dirigidas específicamente a este idWS o globales ("ALL")
                    if(idWS != null && (idWS.equals(record.key()) || "ALL".equals(record.key()))){
                        procesadorMensajes.accept(record.key(), record.value());
                    }
                }
            }
        }catch(Exception e){
            if(running){
                System.out.println("[KAFKA] Error en el consumidor: " + e.getMessage());
            }
        }finally{
            if(consumer!=null){
                consumer.close();
            }
        }
    }
    public void enviarMensaje(String topic, String mensaje){
        if(idWS==null){
            System.err.println("[KAFKA] No se puede enviar el mensaje, idWS no definido.");
            return;
        }
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, idWS, mensaje);
        producer.send(record);
    }
    public void detener(){
        running = false;
        if(producer != null){
            producer.close();
        }
    }
}
