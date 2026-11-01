package wm.central;
import java.net.IDN;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.IntToDoubleFunction;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import wm.common.kafka.Topics;
import wm.common.protocol.EstadoWS;
public class KafkaCentralManager {
    private final String broker;
    private final RegistroEstaciones registro;
    private final BaseDatos bd;
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;
    private volatile boolean running = true;

    //Guarda que operario está regando en que estación: idWS -> idFO
    private final Map<String, String> regandoOperarios = new ConcurrentHashMap<>();
    private final Set<String> pendientesInicio = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    
    public KafkaCentralManager(String broker, RegistroEstaciones registro, BaseDatos bd){
        this.broker=broker;
        this.registro=registro;
        this.bd = bd;
        initProducer();
    }

    private void initProducer(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }

    public void publicarEvento(String topic, String key, String valor){
        producer.send(new ProducerRecord<>(topic, key, valor));
    }

    public void iniciarEscucha(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "central-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(
            Topics.SOLICITAR_RIEGO,
            Topics.INICIO_RIEGO,
            Topics.ESTADO_RIEGO_WS,
            Topics.FIN_RIEGO
        ));
        Thread hilo = new Thread(() -> {
            try{
                while(running){
                    ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(300));
                    for(ConsumerRecord<String,String> record: records){
                        procesarMensaje(record.topic(),record.value());
                    }
                }
            }catch(Exception e){
                if(running){
                    System.err.println("[KAFKA CENTRAL] Error: " + e.getMessage());
                }
            }finally{
                if(consumer!=null){
                    consumer.close();
                }
            }
        }, "central-kafka-listener");
        hilo.start();
    }

    private void procesarMensaje(String topic, String mensaje){
        switch(topic){
            case Topics.SOLICITAR_RIEGO -> procesarSolicitudRiego(mensaje);
            case Topics.INICIO_RIEGO -> procesarInicioRiego(mensaje);
            case Topics.ESTADO_RIEGO_WS -> procesarTelemetriaWS(mensaje);
            case Topics.FIN_RIEGO -> procesarFinRiego(mensaje);
        }
    }
    private void procesarSolicitudRiego(String mensaje){
        String[] partes = mensaje.split("#");
        if(partes.length < 3){
            return;
        }
        String idFO = partes[0];
        String idWS = partes[1];
        int duracion = Integer.parseInt(partes[2]);

        publicarEvento(Topics.RIEGO_PASOS_AUTORIZACION, idFO, "Validando...");
        if(!idFO.equalsIgnoreCase("MANUAL") && !idFO.equalsIgnoreCase("CENTRAL") && !bd.existeOperario(idFO)){
            publicarEvento(Topics.RIEGO_PASOS_AUTORIZACION, idFO, "DENEGADA: Operario no registrado.");
            return;
        }
        
        //Autorizado: ENviar orden y preparar temporizador
        publicarEvento(Topics.RIEGO_PASOS_AUTORIZACION, idFO, "AUTORIZADA: Enviando orden...");
        regandoOperarios.put(idWS, idFO);
        pendientesInicio.add(idWS);

        //Protocolo: INICIAR#<idWS>#<idFO>#<duracion>
        String ordenInicio = String.format("INICIAR#%s#%s#%d", idWS,idFO, duracion);
        publicarEvento(Topics.ORDENES_RIEGO, idWS, ordenInicio);

        //Rechazo: La central espera INICIO 5s y si no llega -> DENEGADA
        scheduler.schedule(()->{
            if(pendientesInicio.contains(idWS)){
                pendientesInicio.remove(idWS);
                regandoOperarios.remove(idWS);
                publicarEvento(Topics.RIEGO_PASOS_AUTORIZACION, idFO, "DENEGADA: La estación no ha respondido.");

            }
        }, 5, TimeUnit.SECONDS);
    }
    private void procesarInicioRiego(String mensaje){
        //Protocolo: INICIO#<idWS>#<idFO>#<timestamp>
        String[] partes = mensaje.split("#");
        if(partes.length >= 4 && partes[0].equals("INICIO")){
            String idWS = partes[1];
            pendientesInicio.remove(idWS);//Cancelamos el timeout

            Estacion est = registro.getEstaciones().stream().filter(e -> e.getId().equals(idWS)).findFirst().orElse(null);
            if(est != null){
                registro.cambiarEstadoPublico(est, EstadoWS.REGANDO);
            }
        }
    }

    private void procesarTelemetriaWS(String mensaje){
        //Protocolo: TELEMETRIA#<idWS>#<caudal>#<volumen>
        String[] partes = mensaje.split("#");
        if(partes.length >= 4 && partes[0].equals("TELEMETRIA")){
            String idWS = partes[1];
            String idFO = regandoOperarios.get(idWS);
            if(idFO !=null){
                publicarEvento(Topics.ESTADO_RIEGO_FO, idFO, mensaje);
            }
        }
    }

    private void procesarFinRiego(String mensaje){
        //Protocolo: FIN#<idWS>#<motivo>#<volumen>#<duracion>
        String[] partes = mensaje.split("#");
        if(partes.length >= 5 && partes[0].equals("FIN")){
            String idWS = partes[1];
            String motivo = partes[2];
            String volumen = partes[3];
            String duracion = partes[4];
            String idFO = regandoOperarios.remove(idWS);
            pendientesInicio.remove(idWS);

            if(idFO!=null){
                publicarEvento(Topics.RESUMEN_RIEGO, idFO,"Resumen: Estación " 
                + idWS + " | Motivo: " + motivo + " | " + volumen + "L en " + 
                duracion + "s");
            }
            Estacion est = registro.getEstaciones().stream().filter(e -> e.getId().equals(idWS)).findFirst().orElse(null);
            if(est != null && est.isConectada()){
                registro.cambiarEstadoPublico(est, est.isBloqueada() ? EstadoWS.FUERA_SERVICIO : EstadoWS.DISPONIBLE);
            }
        }
    }
    public String getOperarioRegando(String idWS){
        return regandoOperarios.get(idWS);
    }

    public void detener(){
        running = false;
        if(producer != null){
            producer.close();
            scheduler.shutdown();
        }
    }
    public void liberarOperario(String idWS){
        regandoOperarios.remove(idWS);
        pendientesInicio.remove(idWS);
    }

}
