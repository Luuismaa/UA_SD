package wm.central;
//Nos permite usar sockets
import java.net.*;

import wm.common.interfaces.IKafkaAdmin;
import wm.common.kafka.KafkaAdmin;
import wm.common.kafka.Topics; 


public class WM_Central {
		//Puerto de escucha de la central
		private static int puertoEscucha;

		//IP y Puerto del broker (Kafka)
		private static String broker;

		//Ruta del archivo de DB
		private static String rutaDB;

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

		//MAIN
    	public static void main(String[] args) throws InterruptedException{

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

			// KAFKA: Instanciamos mediante la interfaz y ejecutamos la inicialización
        	IKafkaAdmin kafkaAdmin = new KafkaAdmin();
        	kafkaAdmin.crearTopicsSiNoExisten(broker, Topics.TODOS);
	}
}
