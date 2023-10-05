package atu.distributed.systems;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

public class TuneifyAPI {
    public static final String PLAYS_ENDPOINT = "/plays";
    private final int port;
    TuneifyKafkaProducer tuneifyKafkaProducer;
    private HttpServer server;

    public TuneifyAPI(int port, TuneifyKafkaProducer tuneifyKafkaProducer) {
        this.port = port;
        this.tuneifyKafkaProducer = tuneifyKafkaProducer;
    }

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        int serverPort = 8000;
        TuneifyAPI tuneifyAPI = new TuneifyAPI(serverPort, new TuneifyKafkaProducer("localhost:9092,localhost:9093,localhost:9094"));
        tuneifyAPI.startServer();
    }
    public void startServer() throws IOException {
        try{
            server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            e.printStackTrace();
        }
        HttpContext playsContext = server.createContext(PLAYS_ENDPOINT);
        playsContext.setHandler(this::handlePlaysRequest);
        server.setExecutor(Executors.newFixedThreadPool(10));
        server.start();
    }

    private void handlePlaysRequest(HttpExchange exchange) throws IOException{
        if( !exchange.getRequestMethod().equalsIgnoreCase("POST")) {
            exchange.close();
            return;
        }
        byte[] reqBytes = exchange.getRequestBody().readAllBytes();
        byte[] respBytes = new byte[0];
        try {
            respBytes = calculateResp(reqBytes);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        sendResponse(respBytes, exchange);

    }

    private byte[] calculateResp(byte[] reqBytes) throws ExecutionException, InterruptedException {
        String bodyString = new String(reqBytes);
        String[] bodyContent = bodyString.split(",");

        long userId = Long.parseLong(bodyContent[0]);
        String artistName = bodyContent[1];
        String trackName = bodyContent[2];
        String albumName = bodyContent[3];


        PlayEvent playEvent = new PlayEvent(userId, artistName, trackName, albumName);
        tuneifyKafkaProducer.publishEvent(playEvent);
        return "OK".getBytes();
    }

    private void sendResponse(byte[] responseBytes, HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(200, responseBytes.length);
        OutputStream os = exchange.getResponseBody();
        os.write(responseBytes);
        os.flush();
        os.close();
    }

    // Used for tests, don't remove
    public void shutdown() {
        System.out.println("Stopping server");
        server.stop(0);
    }
}