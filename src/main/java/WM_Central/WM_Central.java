package src.main.java.WM_Central;
//Nos permite usar sockets
import java.net.*; 


public class WM_Central {
    
        //Recibirá: 1. Puerto de escucha 2. IP del Broker 3. Puerto del Broker
        private static void checkArgs(String[] args) {

            if(args.length != 3) {
				System.out.println("Err args: Indicar puerto de escucha, IP y Puerto del Broker");
				System.exit (1);
            }
        }

    	public static void main(String[] args) {

        checkArgs(args);

		try	{

            //Recolectamos la información
			String puertoEscucha = args[0];
            String ipBroker = args[1];
            String puertoBroker = args[2];

            //Definimos programa como server y configuramos su puerto de escucha
			ServerSocket skServidor = new ServerSocket(Integer.parseInt(puertoEscucha));
		    System.out.println("Escucho el puerto " + puertoEscucha);
	

			while (true)
			{
                // Aceptamos las peticiones (Estaciones)
				Socket skCliente = skServidor.accept();
		        System.out.println("Cliente aceptado!");

                //Aquí debemos crear nuevo hilo por cada estación que venga, y dentro de esa clase creamos la lógica
		        //Thread t = new HiloServidor(skCliente);
		        //t.start();
			}
		}
		catch(Exception e)
		{
			System.out.println("Error: " + e.toString());
			
		}
	}
}
