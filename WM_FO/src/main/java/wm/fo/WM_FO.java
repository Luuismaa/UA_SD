package wm.fo;
import wm.common.kafka.Topics;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.IntToDoubleFunction;
import java.util.List;
import java.util.ArrayList;
import javax.swing.*;
import javax.swing.border.Border;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.time.LocalTime;

public class WM_FO extends JFrame{
    private final String broker;
    private final String idFO;
    private final KafkaFOManager kafkaManager;
    
    //Componentes de la interfaz Swing
    private JComboBox<String> comboEstaciones;
    private JSpinner spinnerDuracion;
    private JButton btnSolicitar;
    private JButton btnCargarFichero;
    private JTextArea txtLog;
    private JLabel lblCaudal;
    private JLabel lblVolumen;
    private JLabel lblEstadoRiego;

    //Table de estaciones globales
    private DefaultTableModel modelTabla;
    private final ScheduledExecutorService executorFichero = Executors.newSingleThreadScheduledExecutor();

    public WM_FO(String broker, String idFO){
        super("Operario de campo - "+idFO);
        this.broker=broker;
        this.idFO=idFO;
        this.kafkaManager = new KafkaFOManager(broker, idFO);
        initGUI();
        iniciarKafka();
    }
    private void initGUI(){
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(850, 600);
        setLocationRelativeTo(null);
        setLayout(new BorderLayout(10, 10));

        //PANEL SUPERIOR: Cabecera e Identificación
        JPanel panelHeader = new JPanel(new FlowLayout(FlowLayout.LEFT));
        panelHeader.setBackground(new Color(45,52,54));
        JLabel lblTitulo = new JLabel("OPERARIO: " +idFO + " | Broker: " + broker);
        lblTitulo.setForeground(Color.WHITE);
        lblTitulo.setFont(new Font("SansSerif", Font.BOLD, 14));
        panelHeader.add(lblTitulo);
        add(panelHeader, BorderLayout.NORTH);

        //PANEL CENTRAL: Izqierda(Acciones y telemetria) - Derecha(Table WS)
        JSplitPane splitCentral = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
        splitCentral.setResizeWeight(0.5);

        //Subpanel Izquierdo: Conrtoles
        JPanel panelIzquierdo = new JPanel(new BorderLayout(10,10));

        //Formulario de solicitud manual
        JPanel panelForm = new JPanel(new GridLayout(4,2,5,5));
        panelForm.setBorder(BorderFactory.createTitledBorder("Solicitud manual de riego"));
        panelForm.add(new JLabel(" Estacion (WS):"));
        comboEstaciones = new JComboBox<>();
        panelForm.add(comboEstaciones);
        panelForm.add(new JLabel(" Duracion (segs)"));
        spinnerDuracion = new JSpinner(new SpinnerNumberModel(30,5,3600,5));
        panelForm.add(spinnerDuracion);
        btnSolicitar = new JButton("Solicitar Riego");
        btnSolicitar.setBackground(new Color(46,204,113));
        btnSolicitar.setForeground(Color.WHITE);
        btnSolicitar.setFont(new Font("SansSerif", Font.BOLD,12));
        btnSolicitar.addActionListener(e->enviarSolicitudManual());
        panelForm.add(btnSolicitar);
        btnCargarFichero = new JButton("Ejecutar Fichero...");
        btnCargarFichero.addActionListener(e -> cargarFicheroProcesos());
        panelForm.add(btnCargarFichero);

        panelIzquierdo.add(panelForm, BorderLayout.NORTH);

        //Panel de telemetria activa
        JPanel panelTelemetria = new JPanel(new GridLayout(3,1,5,5));
        panelTelemetria.setBorder(BorderFactory.createTitledBorder("Telemetria en tiempo real"));
        lblEstadoRiego = new JLabel("Estado: en reposo", SwingConstants.CENTER);
        lblEstadoRiego.setFont(new Font("SansSerif", Font.BOLD, 13));
        lblCaudal = new JLabel("Caudal: -- L/min", SwingConstants.CENTER);
        lblVolumen = new JLabel("Volumen acumulado: -- L", SwingConstants.CENTER);
        panelTelemetria.add(lblEstadoRiego);
        panelTelemetria.add(lblCaudal);
        panelTelemetria.add(lblVolumen);
        panelIzquierdo.add(panelTelemetria, BorderLayout.CENTER);

        splitCentral.setLeftComponent(panelIzquierdo);

        //Subpanel Derecho: Tabla de WS disponibles
        String[] columnas = {"ID Estación", "Ubicación", "Estado"};
        modelTabla = new DefaultTableModel(columnas, 0){
            @Override 
            public boolean isCellEditable(int row, int col){
                return false;
            }
        };
        JTable tablaWS = new JTable(modelTabla);
        JScrollPane scrollTabla = new JScrollPane(tablaWS);
        scrollTabla.setBorder(BorderFactory.createTitledBorder("Red Global de Estaciones (WS)"));
        
        splitCentral.setRightComponent(scrollTabla);
        add(splitCentral, BorderLayout.CENTER);

        //PANEL INFERIOR: Consola de mensajes
        txtLog = new JTextArea(8,50);
        txtLog.setEditable(false);
        txtLog.setFont(new Font("Monospaced", Font.PLAIN, 11));
        JScrollPane scrollLog = new JScrollPane(txtLog);
        scrollLog.setBorder(BorderFactory.createTitledBorder("Registro de aplicación / Mensajes central"));
        add(scrollLog, BorderLayout.SOUTH);
    }
    private void iniciarKafka(){
        log("COnectando a Kafka...");
        kafkaManager.iniciarEscucha((topic,mensaje)->
            SwingUtilities.invokeLater(() ->
                procesarEvento(topic,mensaje)
        ));
    }
    private void procesarEvento(String topic, String mensaje){
        switch(topic){
            case Topics.RIEGO_PASOS_AUTORIZACION -> log("[AUTORIZACIÓN] "+ mensaje);
            case Topics.ESTADO_RIEGO_FO -> actualizarTelemetria(mensaje);
            case Topics.RESUMEN_RIEGO -> {
                log("[FIN RIEGO] Resumen final: " + mensaje);
                lblEstadoRiego.setText("Estado: Finalizado");
                lblEstadoRiego.setForeground((Color.BLACK));
            }
            case Topics.FUGA_RIEGO -> {
                log("[¡ALERTA FUGA!] "+ mensaje);
                lblEstadoRiego.setText("Estado: ¡FUGA DETECTADA!");
                lblEstadoRiego.setForeground(Color.RED);
            }
            case Topics.ESTADO_GLOBAL_WS -> actualizarEstadoGlobalWS(mensaje);
            
        }
    }
    private void actualizarTelemetria(String mensaje){
        // Formato: TELEMETRIA#idWS#caudal#volumen
        String[] partes = mensaje.split("#");
        if(partes.length >= 4){
            lblEstadoRiego.setText("Regando en estacion: " + partes[1]);
            lblEstadoRiego.setForeground(new Color(39,174,96));
            lblCaudal.setText("Caudal: " + partes[2] + " L/min");
            lblVolumen.setText("Volumen acumulado: " + partes[3] + " L");
        }
    }
    private void actualizarEstadoGlobalWS(String mensaje){
        // Formato: ESTADO_WS#idWS#ubicacion#estado
        String[] partes = mensaje.split("#");
        if(partes.length >=4){
            String id = partes[1];
            String ubi = partes[2];
            String est = partes[3];
            boolean encontrada = false;
            for(int i=0; i <modelTabla.getRowCount(); i++){
                if(modelTabla.getValueAt(i, 0).equals(id)){
                    modelTabla.setValueAt(ubi, i, 1);
                    modelTabla.setValueAt(est, i, 2);
                    encontrada = true;
                    break;
                }
            }
            if(!encontrada){
                modelTabla.addRow(new Object[]{id,ubi,est});
                comboEstaciones.addItem(id);
            }
        }
    }
    private void enviarSolicitudManual(){
        String estacionSel = (String) comboEstaciones.getSelectedItem();
        if(estacionSel ==null){
            JOptionPane.showMessageDialog(this, "Seleccione una estación de la lista.", 
                "Aviso",JOptionPane.WARNING_MESSAGE);
            return;
        }
        int duracion = (Integer) spinnerDuracion.getValue();
        log("Enviando solicitud a CENTRAL para " + estacionSel + " (" + duracion +
            " s)...");
        kafkaManager.solicitarRiego(estacionSel, duracion);
    }
    private void cargarFicheroProcesos(){
        JFileChooser chooser = new JFileChooser();
        if(chooser.showOpenDialog(this) == JFileChooser.APPROVE_OPTION){
            File file = chooser.getSelectedFile();
            procesarFichero(file);
        }
    }
    private void procesarFichero(File file){
        log("Leyendo fichero de automatización: " + file.getName());
        List<String> lineasValidas = new ArrayList<>();
        try(BufferedReader reader = new BufferedReader(new FileReader(file))){
            String linea;
            while((linea = reader.readLine()) != null){
                linea = linea.trim();
                if(!linea.isEmpty() && !linea.startsWith("#")){
                    lineasValidas.add(linea);
                }
            }
            //Iniciar procesamiento secuencial con pausa programada de 4 segundos
            programarSiguienteOrden(lineasValidas, 0);
        }catch(Exception e){
            log("ERR al procesar fichero: " + e.getMessage());
        }
    }
    private void programarSiguienteOrden(List<String> lineas, int indice){
        if(indice >= lineas.size()){
            SwingUtilities.invokeLater(() -> log("[FICHERO] Finalizada la ejecución del archivo"));
            return;
        }
        String linea = lineas.get(indice);
        String[] partes = linea.split("#");
        // Formato esperado en fichero: WS-03#60
        if(partes.length >=2){
            String idWS = partes[0];
            int duracion = Integer.parseInt(partes[1]);
            SwingUtilities.invokeLater(() -> log("[FICHERO] Solicitando riego en " + idWS + "por " + duracion +"s"));
            kafkaManager.solicitarRiego(idWS, duracion);
            //Siguiente orden 4 segundos despues mediante el planificador
            executorFichero.schedule(() -> programarSiguienteOrden(lineas, indice + 1), 4, TimeUnit.SECONDS);
        }
        else{
            //Si hay una linea con formato incorrecto salta de inmediato a la siguiente
            programarSiguienteOrden(lineas, indice+1);
        }
    }

    private void log(String mensaje){
        txtLog.append("[" + LocalTime.now().withNano(0) + "]" + mensaje + "\n");
        txtLog.setCaretPosition(txtLog.getDocument().getLength());
    }
    private static void checkArgs(String[] args){
        if(args.length != 2 || !args[0].contains(":")){
            System.err.println("Uso: WM_FO <Broker_IP:Puerto> <ID_Operacion");
            System.err.println("Ejemplo: WM_FO localhost:9092 FO-01");
            System.exit(1);
        }
    }
    public static void main(String[] args){
        checkArgs(args);
        String broker = args[0];
        String idFO = args[1];

        SwingUtilities.invokeLater(() -> {
            WM_FO frame = new WM_FO(broker, idFO);
            frame.setVisible(true);
        });
    }
}

