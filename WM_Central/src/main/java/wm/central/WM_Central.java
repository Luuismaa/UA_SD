package wm.central;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import wm.common.interfaces.IKafkaAdmin;
import wm.common.kafka.KafkaAdmin;
import wm.common.kafka.Topics;
import wm.common.network.SocketServer;


public class WM_Central {
		//Puerto de escucha de la central
		private static int puertoEscucha;

		//IP y Puerto del broker (Kafka)
		private static String broker;

		//Ruta del archivo de DB
		private static String rutaDB;

		//Segundos entre intentos de conexión con Kafka (cada intento ya tarda hasta 15 s por el timeout del AdminClient)
		private static final int REINTENTO_KAFKA_S = 5;

		//Planificador de tareas: ningún hilo se queda parado esperando. En vez de dormir esperando,
		//le pedimos que ejecute una tarea más tarde y el hilo actual queda libre
		private static final ScheduledExecutorService planificador = Executors.newSingleThreadScheduledExecutor();

		//Nº de intento de conexión con Kafka (solo para mostrarlo en el log)
		private static int intentosKafka = 0;

        //Recibirá: 1. Puerto de escucha 2. IP del Broker:Puerto del Broker (Kafka) y SQLite ruta
        private static void checkArgs(String[] args) {

            if(args.length != 3) {
				System.err.println("ERR: <Puerto Escucha> <Puerto:IP de Kafka> <Ruta DB>");
				System.exit (1);
            }

			if(!args[1].contains(":")) {
				System.err.println("ERR: <Puerto Escucha> <Puerto:IP de Kafka> <Ruta DB>");
				System.exit (1);
			}
        }

		/**
		 * Un intento de conectar con Kafka y crear los topics. Si falla, no espera: programa el
		 * siguiente intento en el planificador y termina. Se ejecuta en el hilo del planificador,
		 * así que la Central sigue atendiendo sockets aunque Kafka no esté disponible.
		 */
		private static void inicializarKafka() {
			intentosKafka++;
			try {
				IKafkaAdmin kafkaAdmin = new KafkaAdmin();
				kafkaAdmin.crearTopicsSiNoExisten(broker, Topics.TODOS);
				System.out.println("[KAFKA] Conectado con Kafka en " + broker);
				//Aquí se arrancarán los consumidores/productores de Kafka cuando existan
			} catch (Exception e) {
				System.out.println("[KAFKA] No se ha podido conectar con Kafka (intento " + intentosKafka + "). Reintentando en " + REINTENTO_KAFKA_S + " s...");
				planificador.schedule(WM_Central::inicializarKafka, REINTENTO_KAFKA_S, TimeUnit.SECONDS);
			}
		}

		//MAIN
    	public static void main(String[] args) {

			checkArgs(args);

			try	{
				//Recolectamos la información
				puertoEscucha = Integer.parseInt(args[0]);
				broker = args[1];
				rutaDB = args[2];

			} catch (Exception ex) {
				System.err.println("ERROR con el parseo del puerto, procura que sea un número" + ex);
				System.exit(1);
			}

			//KAFKA: en segundo plano y con reintentos programados. No bloquea el arranque: si Kafka
			//no está, los Monitors pueden autenticarse igualmente (un fallo solo afecta a su servicio)
			planificador.execute(WM_Central::inicializarKafka);

			//ESTACIONES: todas empiezan DESCONECTADA hasta que su Monitor se autentica
			RegistroEstaciones registro = new RegistroEstaciones();
			//TEMPORAL: estaciones fijas hasta que se carguen desde SQLite (rutaDB)
			registro.registrar("WS-01", "Parque Central");
			registro.registrar("WS-02", "Jardín Norte");
			registro.registrar("WS-03", "Paseo del Río");
			for (Estacion est : registro.getEstaciones()) {
				System.out.println("[CENTRAL] Estación registrada: " + est);
			}

			//SOCKETS: un hilo por cada Monitor que se conecte.
			//startServer BLOQUEA aquí para siempre: cualquier otro hilo (Kafka, panel...) debe arrancarse antes
			SocketServer servidor = new SocketServer(new ManejadorMonitor(registro));
			try {
				servidor.startServer(puertoEscucha);
			} catch (IOException e) {
				System.err.println("[CENTRAL] No se puede escuchar en el puerto " + puertoEscucha + ": " + e.getMessage());
				System.exit(1);
			}
		}
}
