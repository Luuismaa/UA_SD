# Prompts usados con IA — Luis Manuel Redondo Marín

Asistente: **Claude Code** (Anthropic).

Prompts agrupados por tema. Se ha corregido la ortografía sin cambiar el contenido; cuando se adjuntó un documento,
una captura o la salida de la terminal, se indica entre corchetes.

---

## 1. Estructura del proyecto y Maven


2. ¿Qué diferencia hay entre un jar y un shade? No sé lo que son.
3. Entonces, ¿ahora mismo está toda la estructura bien? Hago un add, commit y push, y luego merge con develop.
4. Ya tengo Maven instalado; he ejecutado lo que me has pedido y la terminal ha devuelto esto:
   *[Adjunto la salida: compilación correcta de los cuatro módulos y la lista de jars generados.]*
   Varias cosas: ¿qué significa que tenga Maven instalado? A la hora de exponer la práctica, con Maven me garantizo
   que las dependencias estarán bien gestionadas, ¿no? Es decir, en los ordenadores de la universidad, con Docker y
   contenedores. ¿Me das luz verde para subirlo a Git?
5. Todo subido a GitHub: la refactorización del esqueleto, los POM y los paquetes cambiados. Tengo una duda: siempre
   que toco algo me sale esta notificación de VS Code, ¿qué significa? *[Adjunto una captura.]*
6. Tengo un problema con los imports: me dice que los `org.apache` no se pueden resolver. ¿Eso quiere decir que en mi
   entorno no está instalado? ¿No se supone que Maven controla las dependencias y las librerías de Kafka?
7. Me ha fallado al compilar con Maven.
8. Quiero probar a ejecutar la Central. No lo hagas tú, solo dime cómo debo hacerlo con Maven y Java; supongo que no
   será solo `javac WM_Central` más los argumentos.

## 2. Kafka: topics y conexión de la Central

1. Estoy creando los topics. ¿Puedes ayudarme a comentarlos? Primero indica la clave, luego la dirección y luego el
   contenido, por encima. *[Adjunto el documento del protocolo de comunicación.]*
3. En `crearTopicsSiNoExisten`, ¿son necesarios los cuatro argumentos? ¿Qué son las particiones y las réplicas? Si
   siempre van a ser uno, puedo eliminarlas y poner un 1 al crear el topic, ¿no?
4. ¿Por qué, si el atributo no es `static`, no me deja asignarlo? Con `broker` me da error.
5. ¿Cómo ves mi forma de conectar la Central con Kafka? No sabía que `Thread.sleep` lanza una excepción: ¿debería
   preocuparme o solo lo pongo en un try-catch?
6. Tenemos un problema: he ejecutado la Central sin levantar Kafka a propósito. Pensaba que solo saldrían por la
   terminal los intentos, pero sale un montón de texto que no me interesa. Además, no se imprime ningún intento, solo
   al final uno que dice "intento 11/10", que no tiene sentido.
7. Listo: al levantar Kafka ha funcionado y se han creado los topics, pero sigue poniendo "41/10". No sé por qué no
   suma; lo trata como si fuera texto.
8. ¡Genial, ya está! Y cuando la vuelvo a iniciar, los topics ya están creados en Kafka. Ahora, para garantizar la
   resiliencia del programa: ¿qué sucede si Kafka se cae en mitad de la ejecución? ¿Qué lo puede causar? Si lo hago
   a mano, ¿cómo podría hacer que la Central se enterase?

## 3. Planificación y seguimiento del proyecto

1. ¿Qué contexto tienes sobre la práctica? Es para crear el contexto.
2. Te paso todo. ¿Qué me recomiendas hacer ahora que ya tenemos Kafka con los topics: SQLite, sockets o Railway?
   *[Adjunto el enunciado y el documento del protocolo.]*
3. Mira lo que ha subido Jaime y actualiza qué falta para acabar el proyecto con una checklist.
4. Todo en marcha, veo la Central. Antes de hacer nada más: ¿qué queda por hacer de la checklist de antes?

## 4. Sockets entre la Central y el Monitor

1. Te adjunto también cómo nos van a evaluar. Vamos a construir los sockets en la Central para poder unirlo con la
   parte de Jaime. Dime todo lo que tengo que saber. Utiliza pseudocódigo
   para que pueda entenderlo.
   *[Adjunto la guía de corrección.]*


2. Tengo un problema: la clase `SocketClient` debe implementar `receiveMessage`, pero lo hemos movido a
   `ConexionesTramas`.

3. He levantado el contenedor de Kafka, pero no me arranca la Central.

4. Haz el .txt. Una pregunta: ¿qué hace ahora mismo el Monitor? Cuando lo ejecuto simplemente se autentica, pero no
   se puede enviar nada. ¿Necesita una interfaz gráfica?
5. Entonces, ¿todo lo relacionado con los sockets entre la Central y el Monitor está terminado? ¿Cuál sería el
    siguiente paso?

## 5. Eliminación de `sleep`

1. Jaime ha hecho el Engine. Algo muy importante que puedes añadir al contexto: el profesor ha prohibido usar
   `sleep` en toda la práctica. Localiza todos los `sleep` del proyecto y dime qué alternativas tenemos para
   reemplazarlos. Supongo que el objetivo del profesor es que nada se quede colgado esperando, porque en la vida
   real eso no sucede, así que habrá que pensar en otra estrategia.

## 6. Base de datos SQLite

1. Vamos allá. Dime todo lo que tengo que saber sobre SQLite: cómo conectarla, dónde tiene que estar y alguna recomendación.
2. Las decisiones: 1) sí; 2) primero el script y luego el menú; 3) lo más coherente es que, si cambia la ubicación,
   se respete y se reemplace la vieja por la nueva en la BD; no sé qué opinas tú, pero yo pienso eso. Ya he añadido
   `.db` al `.gitignore`. Ahora vamos a crear la BD: ¿dónde tengo que crearla, en la carpeta de `WM_Central`?


## 7. Despliegue: Docker, Railway y AWS


1. *[Railway no permite crear cuentas: adjunto el tutorial de AWS que nos ha pasado el profesor para hacerlo allí.]*
2. Necesito tu ayuda porque estoy perdidísimo. En el paso 1 pone "lanzar instancia" y ponerle de nombre Kafka, pero
   no entiendo nada: ¿dónde pongo el nombre en la terminal? *[Adjunto una captura.]*
3. Listo, ya tengo `Broker-Kafka` en ejecución.
4. Una duda: ¿le doy a "permitir que se reasocie"? *[Adjunto una captura.]*
5. Contenedor creado, pero en estado "Restarting".
6. *[Adjunto los logs del contenedor de Kafka y la memoria disponible de la máquina.]*
7. Me sale "username and password required". En el nombre del secret puse mi usuario y en el valor, el token de
   acceso que me generó Docker Hub. *[Adjunto una captura.]*
8. Estaba creando la máquina de la Central y creo que la he liado: le he dado a "Allocate Elastic IP address" y creo
    que ha cambiado la de antes. *[Adjunto una captura.]*


## 8. Interfaz de la Central (panel web)

1. Pasemos a la interfaz de la Central. Primero, ¿puedes recordarme por qué la de Jaime no se ajustaba a la práctica?
2. Ya lo he visto. Está bien, pero hay cosas que no funcionan: por ejemplo, activo una estación, le doy a regar y me
   sale denegado. *[Adjunto una captura.]*

## 9. Git y control de versiones

1. Se me ha liado: al ir a subirlo todo, había algo en `develop` de lo que no había hecho pull; he hecho merge con mi
   rama y me ha saltado un error en el merge.
2. Se me ha liado otra vez: he intentado hacer merge de mi rama en `develop`, que se suponía que estaba igual que en
   GitHub, pero al hacer el push me ha fallado.
3. *[Adjunto el error de `git pull`: "Diverging branches can't be fast-forwarded".]*

## 10. Preparación de la entrega

1. Actualiza el README.
2. Coge todos mis prompts y mételos en la carpeta, pero no hace falta que crees un .txt por cada uno, que sería mucho.
