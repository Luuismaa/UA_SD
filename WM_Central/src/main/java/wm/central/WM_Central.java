package wm.central;

import javax.swing.*;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.sql.SQLException;
import java.util.Locale;

import wm.common.protocol.EstadoWS;

/**
 * Punto de entrada de la Central y su ventana de escritorio (Swing).
 *
 * La lógica vive en CentralCore. Si hay pantalla se abre esta ventana; si no la hay
 * (Railway, Docker) la Central funciona igual en modo consola: todo sale por los logs.
 */
public class WM_Central extends JFrame {

		private final CentralCore central;

		//Componentes UI
		private JTable tablaEstaciones;
		private DefaultTableModel modeloTabla;
		private JTextArea consola;
		private boolean parpadeoEstado = true; // Para el VERDE PARPADEANDO
		private long eventosMostrados = -1;    // Para repintar la consola solo si hay eventos nuevos

		public WM_Central(CentralCore central){
			this.central = central;
			configurarUI();
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

			//Timer para actualizar la tabla, la consola y alternar el parpadeo visual cada 500ms
			Timer timerUI = new Timer(500, e -> {
				parpadeoEstado = !parpadeoEstado;
				actualizarTabla();
				actualizarConsola();
        	});
        	timerUI.start();
		}

		/**
		 * La consola muestra los mismos eventos de la Central que el panel web (RegistroEventos).
		 */
		private void actualizarConsola(){
			long total = RegistroEventos.getTotal();
			if(total == eventosMostrados){
				return;
			}
			eventosMostrados = total;
			consola.setText(String.join("\n", RegistroEventos.getLineas()));
			consola.setCaretPosition(consola.getDocument().getLength());
		}

		private void log(String mensaje){
			//Pasa por la consola, así queda en RegistroEventos y se ve en la ventana y en el panel web
			System.out.println("[" + java.time.LocalTime.now().withNano(0) + "] " + mensaje);
		}

		private void actualizarTabla(){
			int filaSel = tablaEstaciones.getSelectedRow();
        	String idSel = filaSel != -1 ? (String) modeloTabla.getValueAt(filaSel, 0) : null;
			modeloTabla.setRowCount(0);
			int nuevaSel = -1;
			for(Estacion est: central.getRegistro().getEstaciones()){
				String operario = est.getOperario() != null ? est.getOperario() : "-";
				modeloTabla.addRow(new Object[]{
					est.getId(),
					est.getUbicacion(),
					est.getEstado(),
					String.format(Locale.ROOT, "%.1f", est.getCaudal()),
					String.format(Locale.ROOT, "%.1f", est.getVolumen()),
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

			if(!central.isKafkaConectado()){
				JOptionPane.showMessageDialog(this, "Kafka aún no se ha conectado.", "Error", JOptionPane.ERROR_MESSAGE);
            	return;
			}
			String input = JOptionPane.showInputDialog(this, "Duración del riego en segundos para " + id + ":");
			if(input != null && !input.trim().isEmpty()){
				try {
					int duracion = Integer.parseInt(input.trim());
					if(duracion <= 0){
						throw new NumberFormatException();
					}
					log("[CENTRAL] Solicitando riego en " + id + " por " + duracion + "s.");
					central.solicitarRiego(id, duracion);
				} catch (NumberFormatException ex) {
					JOptionPane.showMessageDialog(this, "La duración debe ser un número entero positivo.", "Error", JOptionPane.ERROR_MESSAGE);
				}
			}
		}

		private void accionarCambioBloqueo(boolean bloquear){
			String id = obtenerIdSeleccionado();
			if (id == null) return;
			if(bloquear){
				log("[CENTRAL] Aplicando bloqueo a " + id);
            	central.bloquear(id);
			}
			else{
				log("[CENTRAL] Aplicando activación a " + id);
            	central.activar(id);
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

		//Puerto del panel web si no se indica el 4º argumento
		private static final int PUERTO_WEB_POR_DEFECTO = 8080;

        /**
         * Recibirá: 1. Puerto de escucha 2. IP:Puerto del Broker (Kafka) 3. Ruta de la BD SQLite
         * 4. (opcional) Puerto del panel web, 8080 por defecto
         */
        private static CentralCore crearDesdeArgs(String[] args) {
			String uso = "ERR: <Puerto Escucha> <IP:Puerto de Kafka> <Ruta DB> [Puerto panel web, por defecto 8080]";
            if((args.length != 3 && args.length != 4) || !args[1].contains(":")) {
				System.err.println(uso);
				System.exit(1);
            }
			try	{
				int puertoWeb = args.length == 4 ? Integer.parseInt(args[3]) : PUERTO_WEB_POR_DEFECTO;
				return new CentralCore(Integer.parseInt(args[0]), args[1], args[2], puertoWeb);
			} catch (NumberFormatException ex) {
				System.err.println("ERR: los puertos deben ser números. " + uso);
				System.exit(1);
				return null; //Nunca se llega aquí: System.exit termina el programa
			}
        }

		//MAIN
	    public static void main(String[] args) {
			//Desde aquí, todo lo que se imprime queda también en el registro de eventos (panel web y ventana)
			RegistroEventos.instalar();
			CentralCore central = crearDesdeArgs(args);
			try {
				central.arrancar();
			} catch (SQLException e) {
				System.err.println("[BD] No se puede abrir la base de datos '" + central.getRutaDB() + "': " + e.getMessage());
				System.exit(1);
			}
			//En AWS/Docker no hay pantalla: la Central sigue funcionando y se maneja desde el panel web
			if (GraphicsEnvironment.isHeadless()) {
				System.out.println("[CENTRAL] Sin pantalla: modo consola (sin ventana)");
			} else {
				SwingUtilities.invokeLater(() -> new WM_Central(central).setVisible(true));
			}
		}
}
