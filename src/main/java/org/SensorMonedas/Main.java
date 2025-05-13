package org.SensorMonedas;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.json.JSONObject;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;


/**
 * Para la immplementación completa se considera la siguiente estructura:
 *
 * ComunicacionesRedes
 *     /CGMO-Proyecto2
 *        /Criptmonedas
 *        /Configuracion
 *
 * Para ver todos los mensajes ingresar a: https://mqttx.app/web-client#/recent_connections y realizar la conexión
 * Se recomienda, para ver todas los mensajes, hacer la subscripción como: ComunicacionesRedes/CGMO-Proyecto2/+
 */
public class Main {
    /** Lista de criptomonedas a monitorear (usar Arrays.asList permite inicializar la lista con las criptomonedas a consultar)
     Dice la documentación del método Array.asList:
     Returns a fixed-size list backed by the specified array. (Changes to the returned list "write through" to the array.) This method acts as bridge between array-based and collection-based APIs, in combination with Collection.toArray(). The returned list is serializable and implements RandomAccess.
     This method also provides a convenient way to create a fixed-size list initialized to contain several elements:      List<String> stooges = Arrays.asList("Larry", "Moe", "Curly");

     Para llenar la lista de criptomonedas a usar es necesario ver que:
     - El url base es wss://stream.binance.com:9443 (WebSocket Secure)
     - Dado que deseamos un flujo combinado usamos: /stream?streams=<streamName1>/<streamName2>/<streamName3> donde los streamNames corresponden a los símbolos de las criptomonedas:
     Por ejemplo: para bitcoin usamos el símbolo btcusdt@ticker
     Aquí es importante considerar que todos los símbolos deben encontrarse en minúsculas
     Como podemos ver en la documentación de Binance, la respuesta en el stream ticker (con actualización cada un segundo) sigue el siguiente formato:
         {
         "e": "24hrTicker",  // Event type
         "E": 1672515782136, // Event time
         "s": "BNBBTC",      // Symbol
         "p": "0.0015",      // Price change
         "P": "250.00",      // Price change percent
         "w": "0.0018",      // Weighted average price
         "x": "0.0009",      // First trade(F)-1 price (first trade before the 24hr rolling window)
         "c": "0.0025",      // Last price
         "Q": "10",          // Last quantity
         "b": "0.0024",      // Best bid price
         "B": "10",          // Best bid quantity
         "a": "0.0026",      // Best ask price
         "A": "100",         // Best ask quantity
         "o": "0.0010",      // Open price
         "h": "0.0025",      // High price
         "l": "0.0010",      // Low price
         "v": "10000",       // Total traded base asset volume
         "q": "18",          // Total traded quote asset volume
         "O": 0,             // Statistics open time
         "C": 86400000,      // Statistics close time
         "F": 0,             // First trade ID
         "L": 18150,         // Last trade Id
         "n": 18151          // Total number of trades
         }
     En particular para nuestro caso es relevante el símbolo:criptomoneda y c (minúsucula): Último precio de mercado.
     Para más información seguir los siguientes links:
     https://docs.oracle.com/javase/8/docs/api/java/util/Arrays.html#asList-T...-
     https://www.binance.com/en/support/faq/detail/b5dcf3f29baa48cea3ee6eb4d6326257
     https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
     **/
    private static final List<String> CRYPTOS = Arrays.asList("btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt", "pepeusdt", "dogeusdt", "adausdt");

    /** Construcción del endpoint para WebSocket
     Ver: https://www.w3schools.com/java/ref_string_join.asp
     Paso a paso:
     - CRYPTOS.stream():
        Convierte la lista en un flujo de datos (Stream) para trabajar en paralelo y hacer transformaciones.
     - .map(c -> c + "@markPrice@1s") [Ver: https://www.geeksforgeeks.org/stream-map-java-examples/] :
        A cada elemento del Stream (representado por c), se le añade el sufijo @ticker.
     - .toArray(String[]::new):
        Convierte el Stream de vuelta a un arreglo de tipo String[].
     - String.join("/", ...):
        Une todos los elementos del arreglo en un solo String, separados por /.
     **/
    private static final String STREAM_URL = "wss://stream.binance.com:9443/stream?streams=" +
            String.join("/", CRYPTOS.stream().map(c -> c + "@ticker").toArray(String[]::new));
    private static String pubTopic;
    public static void main(String[] args) {
        String broker = "tcp://broker.emqx.io:1883";
        String clientId = "SensorMonedas";
        String topic = "ComunicacionesRedes/CGMO-Proyecto2";
        int subQos = 1;
        int pubQos = 1;

        try {
            MqttClient client = new MqttClient(broker, clientId, new MemoryPersistence());
            MqttConnectOptions options = new MqttConnectOptions();
            client.connect(options);
            options.setConnectionTimeout(60);
            options.setKeepAliveInterval(60);

            if (client.isConnected()) {
                client.setCallback(new MqttCallback() {
                    public void messageArrived(String topic, MqttMessage message) throws Exception {
                        System.out.println("topic: " + topic);
                        System.out.println("qos: " + message.getQos());
                        System.out.println("message content: " + new String(message.getPayload()));
                    }

                    public void connectionLost(Throwable cause) {
                        System.out.println("connectionLost: " + cause.getMessage());
                    }

                    public void deliveryComplete(IMqttDeliveryToken token) {
                        System.out.println("deliveryComplete: " + token.isComplete());
                    }
                });

                client.subscribe(topic, subQos);
                //El cliente WebSocket se crea con el URL creado anteriormente
                WebSocketClient wsclient = new WebSocketClient(new URI(STREAM_URL)) {

                    @Override
                    public void onOpen(ServerHandshake handshake) {
                        System.out.println("** Conectado a Binance WebSocket **");
                    }
                    @Override
                    public void onMessage(String message) {
                        // Procesar mensaje recibido
                        pubTopic = "ComunicacionesRedes/CGMO-Proyecto2/Criptomonedas";
                        String total=processMessage(message);
                        System.out.println(total);
                        MqttMessage mqmessage = new MqttMessage(total.getBytes());
                        mqmessage.setQos(pubQos);
                        try {
                            client.publish(pubTopic, mqmessage);
                            System.out.println(pubTopic);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public void onClose(int code, String reason, boolean remote) {
                        System.out.println("** Conexión cerrada: " + reason + " **");
                    }

                    @Override
                    public void onError(Exception ex) {
                        System.out.println("** Error: " + ex.getMessage() + " **");
                    }
                };
                wsclient.connect();
            }


        } catch (MqttException | URISyntaxException e) {
            e.printStackTrace();
        }
    }
    // Método para procesar el mensaje recibido
    private static String processMessage(String message) {
        JSONObject json = new JSONObject(message);
        JSONObject data = json.getJSONObject("data");

        String symbol = data.getString("s");
        pubTopic=pubTopic+"/"+symbol;
        String price = data.getString("c");
        return price;
    }
}
