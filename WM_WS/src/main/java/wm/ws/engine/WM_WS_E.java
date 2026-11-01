package wm.ws.engine;
import java.util.Scanner;
/**
 * Clase principal del Engine de la WS (WM_WS_E). 
 * Se encarga de iniciar el monitor y de conectarse a Kafka.
 * WM_WS_E
 */
public class WM_WS_E {
    /**
     * Comprueba los parámetros de entrada y termina el programa si no son correctos.
     * @param args Argumentos recibidos por consola al iniciar la aplicación.
     */
    private static void checkArgs(String[] args){
        String uso = "ERR: Párametros incorrectos. Uso: <IP:Puerto Kafka> <IP:Puerto Monitor>";
        if(args.length != 2){
            System.err.println(uso);
            System.exit(1);
        }
        if(!args[0].contains(":") || !args[1].contains(":")){
            System.err.println(uso);
            System.exit(1);
        }
    }
    /**
     * Inicia el Engine de la WS. Se conecta al monitor y a Kafka.
     * @param args Argumentos recibidos por consola al iniciar la aplicación. Deben ser: <IP:Puerto Kafka> <IP:Puerto Monitor>
     * args[0] = IP:Puerto del servidor Kafka al que conectarse.
     * args[1] = IP:Puerto del monitor al que conectarse.
     */
    public static void main(String[] args){
        checkArgs(args);
        String puertoKafka = args[0];
        String[] ipPuertoMonitor = args[1].split(":");
        String ipMonitor = ipPuertoMonitor[0];
        int puertoMonitor;
        try{
            puertoMonitor = Integer.parseInt(ipPuertoMonitor[1]);
        }catch(NumberFormatException e){
            System.err.println("ERR: El puerto del monitor debe ser un número");
            System.exit(1);
            return;
        }
        Engine engine = new Engine(puertoKafka, ipMonitor, puertoMonitor);
        engine.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> engine.detener()));
        System.out.println("\n--- PANEL DE CONTROL ENGINE ---");
        System.out.println(" 1: Alternar Avería (OK <-> KO)");
        System.out.println(" 2: Iniciar Riego Manual");
        System.out.println(" 3: Detener Riego Manual");
        System.out.println("-------------------------------");
        try(Scanner scanner = new Scanner(System.in)){
            while(true){
                System.out.print("> ");
                String input = scanner.nextLine().trim();
                switch(input){
                    case "1":
                        engine.alternarAveria();
                        break;
                    case "2":
                        System.out.print(" Duración (segundos): ");
                        try {
                            int duracion = Integer.parseInt(scanner.nextLine().trim());
                            engine.iniciarRiegoManual(duracion);
                        } catch (NumberFormatException e) {
                            System.out.println(" ERR: Introduce un número válido");
                        }
                        break;
                    case "3":
                        engine.detenerRiegoManual();
                        break;
                    default:
                        System.out.println(" Opción no reconocida.");
                }
            }
        }
    }
}
