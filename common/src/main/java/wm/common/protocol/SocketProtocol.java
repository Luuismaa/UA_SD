package wm.common.protocol;
//Traducir texto a bytes y viceversa usando el estandar de codificación UTF-8
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
/**
 * Clase que define el protocolo de comunicación por socket, usando el estandar de empaquetado
 * <STX><DATA><ETX><LRC>
 */
public class SocketProtocol{

    //BYTES DE CONTROL

    //Bytes de diálogo (se envían sueltos)

    //Monitor -> Central. ¿Estás ahí?
    public static final byte ENQ  = 0x05;
    //OK. Recibido y correcto
    public static final byte ACK  = 0x06;
    //MAL. Mal recibido.
    public static final byte NACK = 0x15;
    //FINAL COMUNICACIÓN. Antes del close()
    public static final byte EOT  = 0x04;

    //Bytes de trama (Envuelven datos)
    //Aquí empeizan los datos (el mensaje)
    public static final byte STX = 0x02;
    //Aquí acaban los datos (el mensaje)
    public static final byte ETX = 0x03;
    //[STX][DATA][ETX][LRC]

    

    /**
     * Calcula el LRC (Longitudinal Redundancy Check) de un arreglo de bytes.
     * El LRC se calcula usando la operación XOR a los bytes de DATA y el byte ETX.
     * @param data El arreglo de bytes del cual se calculará el LRC.
     * @return El valor del LRC calculado.
     */
    public static byte calculateLRC(byte[] data) {
        byte lrc = 0;
        for (int i = 0; i < data.length; i++) {
            lrc ^= data[i];// XOR de cada byte de DATA (con ^= se hace cast implícito a Byte y por eso funciona)
        }
        lrc ^= ETX;//Incluye el byte ETX en el cálculo del LRC 
        return lrc;
    }
    /**
     * Empaqueta un texto en el formato: <STX><DATA><ETX><LRC>
     * @param text El texto a empaquetar.
     * @return Un arreglo de bytes que representa el mensaje empaquetado.
     */
    public static byte[] buildFrame(String text){
        byte [] data = text.getBytes(StandardCharsets.UTF_8);
        byte [] frame = new byte[1 + data.length + 1+ 1]; // STX + DATA + ETX + LRC
        frame[0] = STX;
        System.arraycopy(data, 0, frame, 1, data.length);// Copia los bytes de DATA al arreglo frame. Uso: System.arraycopy(src, srcPos, dest, destPos, length);
        frame[1 + data.length] = ETX;
        frame[1 + data.length + 1] = calculateLRC(data);
        return frame;
    }
    /**
     * Desempaqueta y valida el frame recibido
     * @param frame El arreglo de bytes que representa el mensaje empaquetado.
     * @return El texto desempaquetado si el frame es válido, o null si es inválido.
     */
    public static String parseFrame(byte[] frame) throws IllegalArgumentException {
        // Validación de tamaño mínimo (<STX><ETX><LRC> = 3 bytes)
        if (frame == null || frame.length < 3) {
            throw new IllegalArgumentException("Frame incompleto o demasiado corto");
        }
        // Validación de STX y ETX
        if (frame[0] != STX) {
            throw new IllegalArgumentException("Frame inválido: No comienza con STX");
        }
        if (frame[frame.length - 2] != ETX) {
            throw new IllegalArgumentException("Frame inválido: No termina con ETX");
        }
        //Extraemos los bytes de frame
        byte[] data = Arrays.copyOfRange(frame, 1, frame.length - 2); // Extrae DATA. Uso: Arrays.copyOfRange(original, from, to) devuelve un nuevo arreglo que contiene los elementos desde el índice from (inclusive) hasta el índice to (exclusive) del arreglo original.
        //Validar checksum LRC
        byte receivedLRC = frame[frame.length - 1];
        byte calculatedLRC = calculateLRC(data);
        if(receivedLRC != calculatedLRC){
            throw new IllegalArgumentException("Frame inválido: LRC no coincide");
        }
        // Convertimos los bytes de DATA a String usando UTF-8
        return new String(data, StandardCharsets.UTF_8);
    }
}