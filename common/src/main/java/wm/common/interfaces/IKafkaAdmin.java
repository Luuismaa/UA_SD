package wm.common.interfaces;

import java.util.Collection;

public interface IKafkaAdmin {
    /**
     * Hace UN intento de crear los topics que falten. Si Kafka no responde, lanza la excepción:
     * quien llama decide si reintenta y cuándo (con un planificador de tareas).
     */
    public void crearTopicsSiNoExisten(String broker, Collection<String> topics) throws Exception;
}
