package wm.common.network;

import wm.common.protocol.SocketProtocol;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

/**
 * Envuelve un Socket ya abierto y habla con el otro extremo usando el protocolo
 * <STX><DATA><ETX><LRC> con confirmación ACK/NACK.
 *
 * La usan los dos lados de la comunicación (Central y Monitor, y después Monitor y Engine),
 * así ambos hablan exactamente igual.
 *
 * Regla de uso: solo UN hilo debe leer de la conexión (recibirMensaje). Varios hilos sí
 * pueden enviar a la vez (enviarMensaje es synchronized). En nuestro protocolo esto se
 * cumple porque la Central nunca inicia mensajes por socket: solo responde.
 */
public class ConexionesTramas {

    //Veces que se reenvía (o se acepta recibir) una trama corrupta antes de rendirse
    private static final int MAX_REINTENTOS = 3;

    //Tamaño máximo de DATA. Si llega algo más largo, es basura o un ataque
    private static final int MAX_DATA = 4096;

    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;

    public ConexionesTramas(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new DataInputStream(socket.getInputStream());
        this.out = new DataOutputStream(socket.getOutputStream());
    }

    /**
     * Tiempo máximo que puede bloquearse una lectura. Si se supera, la lectura lanza
     * SocketTimeoutException. La Central lo usa para detectar que han dejado de llegar latidos.
     * @param ms milisegundos (0 = esperar para siempre)
     */
    public void setTimeout(int ms) throws SocketException {
        socket.setSoTimeout(ms);
    }

    /**
     * Envía un byte de control suelto (ENQ, ACK, NACK o EOT).
     */
    public synchronized void enviarByte(byte b) throws IOException {
        out.write(b);
        out.flush();
    }

    /**
     * Lee un único byte. Si el otro extremo ha cerrado la conexión lanza EOFException
     * en vez de devolver -1, para que nadie lo confunda con un byte válido.
     */
    public byte leerByte() throws IOException {
        int b = in.read();
        if (b == -1) {
            throw new EOFException("El otro extremo ha cerrado la conexión");
        }
        return (byte) b;
    }

    /**
     * Envía un mensaje empaquetado en trama y espera la confirmación.
     *  - ACK  -> recibido bien, terminamos.
     *  - NACK -> llegó corrupto, se reenvía (hasta MAX_REINTENTOS veces).
     *
     * synchronized: el envío completo (trama + esperar ACK) se hace sin que otro hilo se
     * cuele. Si no, el hilo del latido y el de la fuga podrían mezclar sus tramas en el
     * cable o quedarse uno con el ACK del otro.
     *
     * @param texto mensaje con formato CODIGO#campo1#...
     * @throws IOException si la conexión falla o el otro lado rechaza la trama 3 veces
     */
    public synchronized void enviarMensaje(String texto) throws IOException {
        byte[] trama = SocketProtocol.buildFrame(texto);

        for (int intento = 1; intento <= MAX_REINTENTOS; intento++) {
            out.write(trama);
            out.flush();

            byte respuesta = leerByte();
            if (respuesta == SocketProtocol.ACK) {
                return;
            }
            if (respuesta != SocketProtocol.NACK) {
                throw new IOException("Se esperaba ACK o NACK y llegó el byte " + respuesta);
            }
            System.err.println("[TRAMA] NACK recibido, reenviando (" + intento + "/" + MAX_REINTENTOS + ")");
        }
        throw new IOException("El otro extremo ha rechazado la trama " + MAX_REINTENTOS + " veces");
    }

    /**
     * Espera a recibir una trama, comprueba su LRC y responde ACK (bien) o NACK (corrupta).
     * Bloquea hasta que llega algo (o salta el timeout si se configuró con setTimeout).
     *
     * @return el texto del mensaje, o null si el otro extremo se ha despedido con EOT
     * @throws EOFException si la conexión se cierra sin EOT (p. ej. Ctrl+C del otro lado)
     * @throws java.net.SocketTimeoutException si se supera el timeout sin recibir nada
     * @throws IOException si llegan MAX_REINTENTOS tramas corruptas seguidas
     */
    public String recibirMensaje() throws IOException {
        int corruptas = 0;

        while (corruptas < MAX_REINTENTOS) {
            byte b = leerByte();

            //El otro lado se despide de forma ordenada
            if (b == SocketProtocol.EOT) {
                return null;
            }
            //Cualquier cosa antes de un STX es basura: la ignoramos
            if (b != SocketProtocol.STX) {
                continue;
            }

            //Leemos DATA hasta encontrar ETX
            ByteArrayOutputStream data = new ByteArrayOutputStream();
            while ((b = leerByte()) != SocketProtocol.ETX) {
                if (data.size() >= MAX_DATA) {
                    throw new IOException("Trama demasiado larga (más de " + MAX_DATA + " bytes)");
                }
                data.write(b);
            }
            byte lrc = leerByte();

            //Reconstruimos la trama completa y la validamos con el código de SocketProtocol
            byte[] dataBytes = data.toByteArray();
            byte[] trama = new byte[dataBytes.length + 3];
            trama[0] = SocketProtocol.STX;
            System.arraycopy(dataBytes, 0, trama, 1, dataBytes.length);
            trama[trama.length - 2] = SocketProtocol.ETX;
            trama[trama.length - 1] = lrc;

            try {
                String texto = SocketProtocol.parseFrame(trama);
                enviarByte(SocketProtocol.ACK);
                return texto;
            } catch (IllegalArgumentException e) {
                //LRC incorrecto: pedimos que nos la reenvíen
                corruptas++;
                System.err.println("[TRAMA] " + e.getMessage() + ". Enviando NACK (" + corruptas + "/" + MAX_REINTENTOS + ")");
                enviarByte(SocketProtocol.NACK);
            }
        }
        throw new IOException("Recibidas " + MAX_REINTENTOS + " tramas corruptas seguidas");
    }

    /**
     * Cierra el socket (y con él sus flujos). No envía EOT: si queremos despedirnos de forma
     * ordenada, antes hay que llamar a enviarByte(SocketProtocol.EOT).
     */
    public void cerrar() {
        try {
            socket.close();
        } catch (IOException e) {
            //Ya estaba cerrado o roto: no hay nada más que hacer
        }
    }
}
