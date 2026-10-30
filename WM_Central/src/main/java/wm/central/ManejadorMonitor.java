package wm.central;

import wm.common.interfaces.ManejadorConexion;
import wm.common.network.ConexionesTramas;
import wm.common.protocol.SocketProtocol;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.LocalTime;

/**
 * Conversación de la Central con UN Monitor de estación.
 * El SocketServer ejecuta atender() en un hilo propio por cada Monitor que se conecta.
 *
 * Flujo:
 *   1. Saludo: ENQ -> ACK
 *   2. Autenticación: el primer mensaje debe ser AUTH#idWS#ubicacion -> AUTH_OK / AUTH_KO
 *   3. Bucle: LATIDO, FUGA, FUGA_RESUELTA... hasta EOT, caída o falta de latidos
 *   4. Al salir (sea como sea): la estación pasa a DESCONECTADA
 */
public class ManejadorMonitor implements ManejadorConexion {

    //El Monitor envía un LATIDO por segundo: si en 3 s no llega nada, lo damos por caído
    private static final int TIMEOUT_LATIDO_MS = 3000;

    //Compartido por todos los Monitors (un único RegistroEstaciones para toda la Central)
    private final RegistroEstaciones registro;

    public ManejadorMonitor(RegistroEstaciones registro) {
        this.registro = registro;
    }

    @Override
    public void atender(Socket socket) {
        String origen = String.valueOf(socket.getRemoteSocketAddress());
        //null hasta que la autenticación sea correcta: así el finally solo desconecta
        //estaciones que de verdad estaban conectadas a través de ESTA conexión
        String idWS = null;

        try {
            ConexionesTramas conexion = new ConexionesTramas(socket);
            //Cualquier lectura que tarde más de 3 s lanzará SocketTimeoutException
            conexion.setTimeout(TIMEOUT_LATIDO_MS);

            //1) Saludo
            if (conexion.leerByte() != SocketProtocol.ENQ) {
                log(origen, "no ha empezado con ENQ, se cierra la conexión");
                return;
            }
            conexion.enviarByte(SocketProtocol.ACK);

            //2) Autenticación: el primer mensaje tiene que ser AUTH#idWS#ubicacion
            String mensaje = conexion.recibirMensaje();
            if (mensaje == null) {
                return; //Se ha despedido (EOT) sin llegar a autenticarse
            }
            String[] campos = mensaje.split("#", -1);
            if (campos.length != 3 || !campos[0].equals("AUTH") || campos[2].isBlank()) {
                String id = campos.length > 1 ? campos[1] : "?";
                log(origen, "primer mensaje no válido: '" + mensaje + "'");
                conexion.enviarMensaje("AUTH_KO#" + id + "#FORMATO_INVALIDO");
                return;
            }

            String id = campos[1];
            String ubicacion = campos[2];
            RegistroEstaciones.ResultadoAuth resultado = registro.autenticar(id, ubicacion);
            if (resultado != RegistroEstaciones.ResultadoAuth.OK) {
                log(origen, "AUTH de " + id + " rechazado: " + resultado);
                conexion.enviarMensaje("AUTH_KO#" + id + "#" + resultado.name());
                return;
            }
            //A partir de aquí la estación figura como conectada: si algo falla, el finally la desconecta
            idWS = id;
            conexion.enviarMensaje("AUTH_OK#" + idWS);
            log(origen, idWS + " autenticada");

            //3) Bucle de vida
            while (true) {
                mensaje = conexion.recibirMensaje();
                if (mensaje == null) {
                    log(idWS, "se ha despedido (EOT)");
                    break;
                }

                campos = mensaje.split("#", -1);
                //Todos los mensajes de esta fase son CODIGO#idWS, y el id debe ser el de esta conexión
                if (campos.length != 2 || !campos[1].equals(idWS)) {
                    log(idWS, "mensaje ignorado (formato o id incorrecto): '" + mensaje + "'");
                    continue;
                }

                switch (campos[0]) {
                    case "LATIDO":
                        //Nada que hacer: recibirlo ya ha reiniciado el timeout de 3 s
                        break;
                    case "FUGA":
                        registro.marcarFuga(idWS);
                        break;
                    case "FUGA_RESUELTA":
                        registro.resolverFuga(idWS);
                        break;
                    default:
                        log(idWS, "código desconocido, ignorado: '" + campos[0] + "'");
                }
            }

        } catch (SocketTimeoutException e) {
            log(idWS != null ? idWS : origen, "sin mensajes durante " + (TIMEOUT_LATIDO_MS / 1000) + " s, se da por caída");
        } catch (EOFException e) {
            log(idWS != null ? idWS : origen, "conexión cerrada sin despedirse (posible caída del Monitor)");
        } catch (IOException e) {
            log(idWS != null ? idWS : origen, "error de comunicación: " + e.getMessage());
        } finally {
            //4) Salga como salga: si estaba autenticada, pasa a DESCONECTADA.
            //El socket lo cierra el SocketServer al terminar este método.
            if (idWS != null) {
                registro.desconectar(idWS);
            }
        }
    }

    private void log(String quien, String texto) {
        System.out.println("[" + LocalTime.now().withNano(0) + "] [SOCKET " + quien + "] " + texto);
    }
}
