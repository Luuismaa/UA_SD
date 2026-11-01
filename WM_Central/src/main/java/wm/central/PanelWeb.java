package wm.central;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Panel web de la Central: se abre con un navegador en http://IP_DE_LA_CENTRAL:8080
 *
 * En AWS la Central no tiene pantalla, así que este es su panel de monitorización y su menú
 * (punto 11 de la mecánica). Usa el servidor HTTP que trae Java, sin librerías externas.
 *
 *   GET  /             -> la página (panel.html, dentro del jar)
 *   GET  /api/estado   -> estado de todas las WS + últimos eventos, en JSON (la página lo pide cada segundo)
 *   POST /api/accion   -> accion=iniciar|bloquear|activar, id=WS-03|TODAS, duracion=60
 */
public class PanelWeb {

    private final CentralCore central;
    private final int puerto;
    private final byte[] pagina;

    public PanelWeb(CentralCore central, int puerto) throws IOException {
        this.central = central;
        this.puerto = puerto;
        try (InputStream in = PanelWeb.class.getResourceAsStream("/panel.html")) {
            if (in == null) {
                throw new IOException("No se encuentra panel.html dentro del jar");
            }
            this.pagina = in.readAllBytes();
        }
    }

    public void arrancar() throws IOException {
        HttpServer servidor = HttpServer.create(new InetSocketAddress(puerto), 0);
        servidor.createContext("/", this::servirPagina);
        servidor.createContext("/api/estado", this::servirEstado);
        servidor.createContext("/api/accion", this::ejecutarAccion);
        //Varios hilos: el panel puede estar abierto en varios navegadores a la vez
        servidor.setExecutor(Executors.newFixedThreadPool(4));
        servidor.start();
        System.out.println("[PANEL WEB] Panel disponible en http://<IP de la Central>:" + puerto);
    }

    private void servirPagina(HttpExchange ex) throws IOException {
        if (!ex.getRequestURI().getPath().equals("/")) {
            responder(ex, 404, "text/plain", "No encontrado");
            return;
        }
        responder(ex, 200, "text/html; charset=utf-8", pagina);
    }

    /**
     * {"kafka":true, "estaciones":[{...}], "eventos":["...", ...]}
     */
    private void servirEstado(HttpExchange ex) throws IOException {
        StringBuilder json = new StringBuilder("{\"kafka\":").append(central.isKafkaConectado());

        json.append(",\"estaciones\":[");
        boolean primera = true;
        for (Estacion est : central.getRegistro().getEstaciones()) {
            if (!primera) {
                json.append(',');
            }
            primera = false;
            json.append("{\"id\":").append(texto(est.getId()))
                .append(",\"ubicacion\":").append(texto(est.getUbicacion()))
                .append(",\"estado\":").append(texto(est.getEstado().name()))
                .append(",\"caudal\":").append(String.format(Locale.ROOT, "%.1f", est.getCaudal()))
                .append(",\"volumen\":").append(String.format(Locale.ROOT, "%.1f", est.getVolumen()))
                .append(",\"operario\":").append(texto(est.getOperario() != null ? est.getOperario() : ""))
                .append(",\"bloqueada\":").append(est.isBloqueada())
                .append('}');
        }
        json.append("],\"eventos\":[");
        List<String> eventos = RegistroEventos.getLineas();
        for (int i = 0; i < eventos.size(); i++) {
            if (i > 0) {
                json.append(',');
            }
            json.append(texto(eventos.get(i)));
        }
        json.append("]}");

        responder(ex, 200, "application/json; charset=utf-8", json.toString());
    }

    /**
     * Órdenes del punto 11 a una estación o a todas: iniciar riego, bloquear o activar.
     */
    private void ejecutarAccion(HttpExchange ex) throws IOException {
        if (!ex.getRequestMethod().equals("POST")) {
            responder(ex, 405, "text/plain", "Solo POST");
            return;
        }
        Map<String, String> p = leerFormulario(ex);
        String accion = p.getOrDefault("accion", "");
        String id = p.getOrDefault("id", "");

        //Estaciones afectadas: una (si existe) o todas
        List<Estacion> objetivo;
        if (id.equals("TODAS")) {
            objetivo = List.copyOf(central.getRegistro().getEstaciones());
        } else if (central.getRegistro().getEstacion(id) != null) {
            objetivo = List.of(central.getRegistro().getEstacion(id));
        } else {
            responderJson(ex, false, "Estación desconocida: " + id);
            return;
        }

        switch (accion) {
            case "iniciar" -> {
                int duracion;
                try {
                    duracion = Integer.parseInt(p.getOrDefault("duracion", ""));
                } catch (NumberFormatException e) {
                    duracion = -1;
                }
                if (duracion <= 0) {
                    responderJson(ex, false, "La duración debe ser un número de segundos mayor que 0");
                    return;
                }
                if (!central.isKafkaConectado()) {
                    responderJson(ex, false, "Kafka no está disponible: no se pueden iniciar riegos");
                    return;
                }
                //Cada petición pasa por la validación normal: las WS no disponibles se denegarán
                for (Estacion est : objetivo) {
                    central.solicitarRiego(est.getId(), duracion);
                }
                responderJson(ex, true, "Riego de " + duracion + " s solicitado en " + nombre(id));
            }
            case "bloquear" -> {
                objetivo.forEach(est -> central.bloquear(est.getId()));
                responderJson(ex, true, "Bloqueo enviado a " + nombre(id));
            }
            case "activar" -> {
                objetivo.forEach(est -> central.activar(est.getId()));
                responderJson(ex, true, "Activación enviada a " + nombre(id));
            }
            default -> responderJson(ex, false, "Acción desconocida: " + accion);
        }
    }

    private static String nombre(String id) {
        return id.equals("TODAS") ? "todas las estaciones" : id;
    }

    /**
     * Lee un formulario "a=1&b=2" del cuerpo de la petición.
     */
    private static Map<String, String> leerFormulario(HttpExchange ex) throws IOException {
        String cuerpo = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        Map<String, String> parametros = new HashMap<>();
        for (String par : cuerpo.split("&")) {
            String[] kv = par.split("=", 2);
            if (kv.length == 2) {
                parametros.put(URLDecoder.decode(kv[0], StandardCharsets.UTF_8), URLDecoder.decode(kv[1], StandardCharsets.UTF_8));
            }
        }
        return parametros;
    }

    private static void responderJson(HttpExchange ex, boolean ok, String mensaje) throws IOException {
        responder(ex, 200, "application/json; charset=utf-8", "{\"ok\":" + ok + ",\"mensaje\":" + texto(mensaje) + "}");
    }

    private static void responder(HttpExchange ex, int codigo, String tipo, String cuerpo) throws IOException {
        responder(ex, codigo, tipo, cuerpo.getBytes(StandardCharsets.UTF_8));
    }

    private static void responder(HttpExchange ex, int codigo, String tipo, byte[] cuerpo) throws IOException {
        ex.getResponseHeaders().set("Content-Type", tipo);
        ex.getResponseHeaders().set("Cache-Control", "no-store");
        ex.sendResponseHeaders(codigo, cuerpo.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(cuerpo);
        }
    }

    /**
     * Texto como cadena JSON: entre comillas y con los caracteres especiales escapados.
     */
    private static String texto(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }
}
