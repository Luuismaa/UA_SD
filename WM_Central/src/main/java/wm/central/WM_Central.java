package wm.central;

import javax.swing.*;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import wm.common.interfaces.IKafkaAdmin;
import wm.common.kafka.KafkaAdmin;
import wm.common.kafka.Topics;
import wm.common.network.SocketServer;
import wm.common.protocol.EstadoWS;

public class WM_Central extends JFrame {
		//Puerto de escucha de la central
		private static int puertoEscucha;

		//IP y Puerto del broker (Kafka)
		private static String broker;

		//Ruta del archivo de DB
		private static String rutaDB;

		private BaseDatos bd;
		private RegistroEstaciones registro;
		private KafkaCentralManager kafkaManager;

		//Componentes UI
		private JTable tablaEstaciones;
		private DefaultTableModel modeloTabla;
		private JTextArea consola;
		private boolean parpadeoEstado = true; // Para el VERDE PARPADEANDO	

		

		//Segundos entre intentos de conexión con Kafka (cada intento ya tarda hasta 15 s por el timeout del AdminClient)
		private static final int REINTENTO_KAFKA_S = 5;

		//Planificador de tareas: ningún hilo se queda parado esperando. En vez de dormir esperando,
		//le pedimos que ejecute una tarea más tarde y el hilo actual queda libre
		private final ScheduledExecutorService planificador = Executors.newScheduledThreadPool(2);

		//Nº de intento de conexión con Kafka (solo para mostrarlo en el log)
		private static int intentosKafka = 0;

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
			try	{
				//Recolectamos la información
				puertoEscucha = Integer.parseInt(args[0]);
				broker = args[1];
				rutaDB = args[2];

			} catch (Exception ex) {
				System.err.println("ERROR con el parseo del puerto, procura que sea un número" + ex);
				System.exit(1);
			}
        }
		private void configurarUI(){
			setTitle("Centro de Control y Monitorización (WM_Central)");
			setSize(950, 600);
			setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
			setLayout(new BorderLayout(10, 10));

			// Tabla de Estaciones de Riego
			String[] columnas = {"ID", "Ubicación", "Estado", "Caudal (L/min)", "Volumen (L)", "Operario"};
			modeloTabla = new DefaultTableModel(columnas, 0) {
				@Override
				public boolean isCellEditable(int row, int column) { return false; }
			};
			tablaEstaciones = new JTable(modeloTabla);
			tablaEstaciones.setDefaultRenderer(Object.class, new RenderizadorEstaciones());
        	tablaEstaciones.setRowHeight(28);
			JScrollPane scrollTabla = new JScrollPane(tablaEstaciones);
			scrollTabla.setBorder(BorderFactory.createTitledBorder("Estaciones de Riego (WS)"));
			add(scrollTabla, BorderLayout.CENTER);

			//Panel lateral de operaciones
			JPanel panelBotones = new JPanel(new GridLayout(3,1,10,10));
			panelBotones.setBorder(BorderFactory.createTitledBorder("Acciones"));
			JButton btnIniciar = new JButton("Iniciar Riego");
			JButton btnBloquear = new JButton("Bloquear WS");
			JButton btnActivar = new JButton("Activar WS");
			btnIniciar.addActionListener(e -> accionarIniciarRiego());
        	btnBloquear.addActionListener(e -> accionarCambioBloqueo(true));
        	btnActivar.addActionListener(e -> accionarCambioBloqueo(false));
			panelBotones.add(btnIniciar);
       	 	panelBotones.add(btnBloquear);
       		panelBotones.add(btnActivar);
			JPanel panelDerecho = new JPanel(new BorderLayout());
			panelDerecho.add(panelBotones, BorderLayout.NORTH);
			add(panelDerecho, BorderLayout.EAST);

			//Consola de eventos
			consola = new JTextArea(8, 50);
			consola.setEditable(false);
			JScrollPane scrollConsola = new JScrollPane(consola);
			scrollConsola.setBorder(BorderFactory.createTitledBorder("Mensajes de la Aplicación"));
			add(scrollConsola, BorderLayout.SOUTH);	

			//Timer para actualizar la tabla y alternar el parpadeo visual cada 500ms
			Timer timerUI = new Timer(500, e -> {
				parpadeoEstado = !parpadeoEstado;
				actualizarTabla();
        	});
        	timerUI.start();
		}

		private void log(String mensaje){
			SwingUtilities.invokeLater(() -> {
				consola.append(mensaje + "\n");
				consola.setCaretPosition(consola.getDocument().getLength());
				System.out.println(mensaje);
			});
		}
		private void iniciarLogica(){
			// BASE DE DATOS + ESTACIONES: se cargan las WS registradas en SQLite
			bd = new BaseDatos(rutaDB);
			registro = new RegistroEstaciones(bd);
			try {
				bd.inicializar();
				for (BaseDatos.DatosEstacion datos : bd.cargarEstaciones()) {
					registro.registrar(datos.id(), datos.ubicacion(), datos.fueraServicio());
				}
				log("[BD] Carga de base de datos e información completada.");
			} catch (SQLException e) {
				log("[BD] No se puede abrir la base de datos '" + rutaDB + "': " + e.getMessage());
				System.exit(1);
			}
			//KAFKA y SOCKETS en segundo plano para no bloquear el hilo de la UI Swing
			planificador.execute(this::inicializarKafka);
			planificador.execute(this::iniciarSockets);
		}

		/**
		 * Un intento de conectar con Kafka y crear los topics. Si falla, no espera: programa el
		 * siguiente intento en el planificador y termina. Se ejecuta en el hilo del planificador,
		 * así que la Central sigue atendiendo sockets aunque Kafka no esté disponible.
		 */
		private void inicializarKafka() {
			intentosKafka++;
			try {
				IKafkaAdmin kafkaAdmin = new KafkaAdmin();
				kafkaAdmin.crearTopicsSiNoExisten(broker, Topics.TODOS);
				log("[KAFKA] Conectado con Kafka en " + broker);
				kafkaManager = new KafkaCentralManager(broker, registro, bd);
				registro.setKafkaManager(kafkaManager);
				kafkaManager.iniciarEscucha();

				// Publicación periódica del estado de la central a operarios (cada 5s)
				planificador.scheduleAtFixedRate(() -> {
					for (Estacion est : registro.getEstaciones()) {
						String msg = String.format("ESTADO_WS#%s#%s#%s", est.getId(), est.getUbicacion(), est.getEstado().name());
						kafkaManager.publicarEvento(Topics.ESTADO_GLOBAL_WS, est.getId(), msg);
					}
				}, 0, 5, TimeUnit.SECONDS);
				//Aquí se arrancarán los consumidores/productores de Kafka cuando existan
			} catch (Exception e) {
				System.out.println("[KAFKA] No se ha podido conectar con Kafka (intento " + intentosKafka + "). Reintentando en " + REINTENTO_KAFKA_S + " s...");
				planificador.schedule(this::inicializarKafka, REINTENTO_KAFKA_S, TimeUnit.SECONDS);
			}
		}

		private void iniciarSockets(){
			SocketServer servidor = new SocketServer((new ManejadorMonitor(registro)));
			try{
				log("[SOCKETS] Servidor a la escucha en el puerto " + puertoEscucha);
				servidor.startServer(puertoEscucha);
			}catch(IOException e){
				log("[SOCKETS] No se puede escuchar en el puerto " + puertoEscucha + ": " + e.getMessage());
			}
		}

		private void actualizarTabla(){
			if (registro == null) return;
			int filaSel = tablaEstaciones.getSelectedRow();
        	String idSel = filaSel != -1 ? (String) modeloTabla.getValueAt(filaSel, 0) : null;
			modeloTabla.setRowCount(0);
			int nuevaSel = -1;
			for(Estacion est: registro.getEstaciones()){
				String operario = kafkaManager != null ? kafkaManager.getOperarioRegando(est.getId()): null;
				if(operario ==null) operario = "-";
				modeloTabla.addRow(new Object[]{
					est.getId(),
					est.getUbicacion(),
					est.getEstado(),
					"0.0",
					"0",
					operario
				});
				if(est.getId().equals(idSel)){
					nuevaSel = modeloTabla.getRowCount()-1;
				}
			}
			if(nuevaSel != -1){
				tablaEstaciones.setRowSelectionInterval(nuevaSel, nuevaSel);
			}
			else{
				tablaEstaciones.repaint();
			}
		}

		private String obtenerIdSeleccionado(){
			int fila = tablaEstaciones.getSelectedRow();
			if(fila==-1){
				JOptionPane.showMessageDialog(this, "Selecciona primero una estación en la tabla.", "Aviso", JOptionPane.WARNING_MESSAGE);
				return null;
			}
			return (String) modeloTabla.getValueAt(fila, 0);
		}

		private void accionarIniciarRiego(){
			String id = obtenerIdSeleccionado();
        	if (id == null) return;

			if(kafkaManager ==null){
				JOptionPane.showMessageDialog(this, "Kafka aún no se ha conectado.", "Error", JOptionPane.ERROR_MESSAGE);
            	return;
			}
			String input = JOptionPane.showInputDialog(this, "Duración del riego en segundos para " + id + ":");
			if(input != null && !input.trim().isEmpty()){
				try {
					int duracion = Integer.parseInt(input.trim());
					log("[CENTRAL] Solicitando riego manual en " + id + " por " + duracion + "s.");
					kafkaManager.publicarEvento(Topics.SOLICITAR_RIEGO, id, "CENTRAL#" + id + "#" + duracion);
				} catch (NumberFormatException ex) {
					JOptionPane.showMessageDialog(this, "La duración debe ser un número entero.", "Error", JOptionPane.ERROR_MESSAGE);
				}
			}
		}

		private void accionarCambioBloqueo(boolean bloquear){
			String id = obtenerIdSeleccionado();
			if (id == null) return;
			if(bloquear){
				log("[CENTRAL] Aplicando bloqueo a " + id);
            	registro.bloquear(id);
			}
			else{
				log("[CENTRAL] Aplicando activación a " + id);
            	registro.activar(id);	
			}
			actualizarTabla();
		}

		//Renderizado de celdas
		private class RenderizadorEstaciones extends DefaultTableCellRenderer{
			@Override
			public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column){
				Component c = super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
				EstadoWS estado = (EstadoWS) table.getValueAt(row, 2);

				if (!isSelected) {
					switch (estado) {
						case DISPONIBLE -> c.setBackground(new Color(144, 238, 144)); // Verde claro
						case REGANDO -> c.setBackground(parpadeoEstado ? new Color(50, 205, 50) : Color.WHITE); // Verde parpadeante
						case FUGA -> c.setBackground(new Color(255, 99, 71)); // Rojo
						case FUERA_SERVICIO -> c.setBackground(Color.ORANGE); // Naranja
						case DESCONECTADA -> c.setBackground(Color.LIGHT_GRAY); // Gris
						default -> c.setBackground(Color.WHITE);
					}
					c.setForeground(Color.BLACK);
				}
				return c;
			}
		}

		public WM_Central(String[] args){
			checkArgs(args);
			configurarUI();
			iniciarLogica();
		}

		//MAIN
	    public static void main(String[] args) {
			SwingUtilities.invokeLater(() -> new WM_Central(args).setVisible(true));
		}
}
