package wm.central;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Guarda las últimas líneas que la Central escribe por consola (autenticaciones, riegos, fugas...)
 * para enseñarlas en el panel web y en la ventana Swing.
 *
 * En vez de cambiar todos los System.out.println del proyecto, se "envuelve" la consola: cada
 * línea se sigue imprimiendo igual (logs de Docker) y además se guarda aquí.
 */
public final class RegistroEventos {

    //Solo se guardan las últimas líneas: el registro completo está en la consola / docker logs
    private static final int MAX_LINEAS = 200;

    private static final Deque<String> lineas = new ArrayDeque<>();
    //Total de líneas guardadas desde el arranque: sirve para saber si hay líneas nuevas
    private static long total = 0;

    private RegistroEventos() { }

    /**
     * Envuelve System.out y System.err. Se llama una vez, al principio del main.
     */
    public static void instalar() {
        System.setOut(envolver(System.out));
        System.setErr(envolver(System.err));
    }

    private static PrintStream envolver(PrintStream original) {
        return new PrintStream(original, true, StandardCharsets.UTF_8) {
            @Override
            public void println(String linea) {
                super.println(linea);
                anadir(linea);
            }

            @Override
            public void println(Object objeto) {
                super.println(objeto);
                anadir(String.valueOf(objeto));
            }
        };
    }

    private static synchronized void anadir(String linea) {
        if (linea == null || linea.isBlank()) {
            return;
        }
        lineas.addLast(linea);
        if (lineas.size() > MAX_LINEAS) {
            lineas.removeFirst();
        }
        total++;
    }

    /**
     * Las últimas líneas, de la más antigua a la más reciente.
     */
    public static synchronized List<String> getLineas() {
        return new ArrayList<>(lineas);
    }

    public static synchronized long getTotal() {
        return total;
    }
}
