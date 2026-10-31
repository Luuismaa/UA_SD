package wm.common.kafka;

//Imports de Kafka. Nos las añade Maven (en dependecias añadimos kafka.client)
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;

//Imports necesarios para la implementación
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

//Clase de utilidad, no la instanciaremos solo usaremos sus métodos para configurar topics
public final class KafkaAdmin implements wm.common.interfaces.IKafkaAdmin {
    /**
     * Crea los topics que no existan todavía. si ya existen, no hace nada
     * y no lanza error.
     * Hace UN solo intento: los reintentos los programa quien llama (la Central),
     * así solo hay una lógica de reintentos en todo el proyecto.
     *
     * @param broker      "ip:puerto" del broker
     * @param topics      nombres de los topics (p. ej. Topics.TODOS)
     * @throws Exception  si Kafka no responde o falla la creación de algún topic
     */
    @Override
    public void crearTopicsSiNoExisten(String broker, Collection<String> topics) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker); //IP:Puerto
        // Timeouts cortos: si Kafka no responde, preferimos fallar rápido y reintentar
        // desde la Central que quedarnos colgados minutos.
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15_000);

        // try-with-resources: el AdminClient abre conexiones e hilos internos,
        // así que hay que cerrarlo siempre, incluso si salta una excepción.
        try (AdminClient admin = AdminClient.create(props)) {

            // 1) Qué topics hay ya en el clúster
            Set<String> existentes = admin.listTopics().names().get();

            // 2) Nos quedamos solo con los que faltan
            List<NewTopic> nuevos = topics.stream()
                    .filter(t -> !existentes.contains(t))
                    .map(t -> new NewTopic(t, 1, (short) 1))
                    .collect(Collectors.toList());

            if (nuevos.isEmpty()) {
                System.out.println("[KAFKA] Todos los topics ya existen (" + topics.size() + ")");
                return;
            }

            // 3) Petición de creación: devuelve un futuro POR topic
            CreateTopicsResult resultado = admin.createTopics(nuevos);

            for (Map.Entry<String, KafkaFuture<Void>> e : resultado.values().entrySet()) {
                try {
                    e.getValue().get(); // espera a que ESTE topic quede creado
                    System.out.println("[KAFKA] Topic creado: " + e.getKey());
                } catch (ExecutionException ex) {
                    // Los errores del futuro vienen "envueltos": la causa real está en getCause()
                    if (ex.getCause() instanceof TopicExistsException) {
                        // Carrera: alguien lo creó entre el listTopics y el createTopics. Es OK.
                        System.out.println("[KAFKA] Topic ya existía: " + e.getKey());
                    } else {
                        throw ex; // cualquier otro error sí es un problema real
                    }
                }
            }
        }
    }
}
