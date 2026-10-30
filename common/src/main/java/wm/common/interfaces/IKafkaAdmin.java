package wm.common.interfaces;

import java.util.Collection;

public interface IKafkaAdmin {
    public void crearTopicsSiNoExisten(String broker, Collection<String> topics);
}
