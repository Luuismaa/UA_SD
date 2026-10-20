package wm.common.kafka;

import java.util.List;

//Clase que usamos para agrupar los topics. Nada más
public final class Topics {

    //Clave: idWS | FO -> Central y WS -> Central
    //Petición de riego: solicitante (idFO o MANUAL), idWS y duración
    public static final String SOLICITAR_RIEGO = "solicitarRiego";

    //Clave: idWS | Central -> WS
    //Órdenes: INICIAR, PARAR, BLOQUEAR o ACTIVAR
    public static final String ORDENES_RIEGO = "ordenesRiego";

    //Clave: idFO | Central -> FO
    //Pasos del proceso: RECIBIDA, VALIDANDO, ESPERANDO_WS, AUTORIZADA o DENEGADA con motivo
    public static final String RIEGO_PASOS_AUTORIZACION = "riegoPasosAutorizacion";

    //Clave: idWS | WS -> Central
    //Telemetría cada 1 s mientras riega: caudal y volumen acumulado
    public static final String ESTADO_RIEGO_WS = "estadoRiegoWS";

    //Clave: idFO | Central -> FO
    //Telemetría de la WS reenviada cada 1 s al FO del riego en curso
    public static final String ESTADO_RIEGO_FO = "estadoRiegoFO";

    //Clave: idWS | WS -> Central
    //Fin del riego: motivo, volumen total y duración
    public static final String FIN_RIEGO = "finRiego";

    //Clave: idFO | Central -> FO
    //Resumen final del riego: volumen, duración y motivo
    public static final String RESUMEN_RIEGO = "resumenRiego";

    //Clave: idWS | WS -> Central
    //Confirmación de que la electroválvula se ha abierto
    public static final String INICIO_RIEGO = "inicioRiego";

    //Clave: idFO | Central -> FO
    //Aviso de fuga en la WS que le estaba regando
    public static final String FUGA_RIEGO = "fugaRiego";

    //Clave: idWS | Central -> todos los FO
    //Estado y ubicación de cada WS, en cada cambio de estado y cada 5 s
    public static final String ESTADO_GLOBAL_WS = "estadoGlobalWS";


//La Central la usa para crearlos todos al arrancar
    public static final List<String> TODOS = List.of(SOLICITAR_RIEGO, ORDENES_RIEGO, RIEGO_PASOS_AUTORIZACION, ESTADO_RIEGO_WS, ESTADO_RIEGO_FO, FIN_RIEGO, RESUMEN_RIEGO, INICIO_RIEGO, FUGA_RIEGO, ESTADO_GLOBAL_WS);

    private Topics() { }
}
