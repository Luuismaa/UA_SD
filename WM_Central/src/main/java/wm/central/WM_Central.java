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

		private static final int  MAX_INTENTOS = 10;

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

			//KAFKA: intentaremos hasta que se conecte con Kafka
			for (int intentos = 0; intentos < MAX_INTENTOS; intentos++) {
				try {
					//Creamos los Topics conectandonos con Kafka (si no se puede por algún error lo volverá a intentar)
					IKafkaAdmin kafkaAdmin = new KafkaAdmin();
					kafkaAdmin.crearTopicsSiNoExisten(broker, Topics.TODOS);					
					break;
				
			} catch (Exception e) {
					
					System.out.println("[KAFKA] No se ha podido conectar con Kafka. Intento " + (intentos + 1) + "/" + MAX_INTENTOS);

					if(intentos == MAX_INTENTOS-1) {
						System.err.println("[KAFKA] No se ha podido conectar con Kafka. Apagando la central");
						System.exit(1);
					}

					//Esperamos para el próximo intento
					Thread.sleep(1000L * (intentos+1)); //1s, 2s, 3s (más los timeouts de KafkaAdmin)
				}
			}
		}
}
