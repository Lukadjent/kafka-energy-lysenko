import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

/** Local shutdown control for the JVM; System.exit runs Kafka/ZooKeeper hooks. */
public class LabService {
    public static void main(String[] args) throws Exception {
        String mainClass = args[0];
        int controlPort = Integer.parseInt(args[1]);
        String token = args[2];
        ServerSocket control = new ServerSocket();
        control.bind(new InetSocketAddress("127.0.0.1", controlPort));
        Thread shutdown = new Thread(() -> {
            while (!control.isClosed()) {
                try (Socket socket = control.accept()) {
                    socket.setSoTimeout(2000);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                    if (token.equals(in.readLine())) {
                        socket.getOutputStream().write("stopping\n".getBytes(StandardCharsets.UTF_8));
                        socket.getOutputStream().flush();
                        System.exit(0);
                    }
                } catch (IOException ignored) { }
            }
        }, "lab-local-shutdown");
        shutdown.setDaemon(true);
        shutdown.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { control.close(); } catch (IOException ignored) { }
        }));
        try {
            Class.forName(mainClass).getMethod("main", String[].class)
                .invoke(null, (Object) Arrays.copyOfRange(args, 3, args.length));
        } catch (InvocationTargetException error) {
            error.getCause().printStackTrace();
            System.exit(1);
        }
    }
}

