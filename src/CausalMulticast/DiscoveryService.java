package CausalMulticast;

import java.net.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Serviço responsável por descobrir dinamicamente outros processos na rede
 * utilizando UDP Multicast.
 */
public class DiscoveryService extends Thread {
    private int localPort;
    
    /** Lista de membros ativos descobertos na rede. */
    public List<InetSocketAddress> members = new CopyOnWriteArrayList<>();
    
    private MulticastSocket multicastSocket;
    private InetAddress groupAddress;
    private static final String MULTICAST_IP = "224.0.0.1";
    private static final int MULTICAST_PORT = 8888;

    /**
     * Inicializa o serviço de descoberta na porta especificada.
     * @param localPort Porta UDP do processo atual.
     */
    public DiscoveryService(int localPort) {
        this.localPort = localPort;
        try {
            this.multicastSocket = new MulticastSocket(MULTICAST_PORT);
            this.groupAddress = InetAddress.getByName(MULTICAST_IP);
            
            // --- CORREÇÃO DA API DEPRECADA ---
            // Cria o endereço do soquete combinando IP e Porta
            SocketAddress group = new InetSocketAddress(this.groupAddress, MULTICAST_PORT);
            
            // Entra no grupo usando a API moderna (evita o warning do compilador)
            this.multicastSocket.joinGroup(group, null);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Mapeia os IPs e portas para gerar um ID determinístico único (0, 1, 2...).
     * @return O ID inteiro do processo local.
     */
    public int getMyId() {
        List<InetSocketAddress> sorted = new ArrayList<>(members);
        sorted.sort(Comparator.comparing((InetSocketAddress addr) -> addr.getAddress().getHostAddress())
                              .thenComparingInt(InetSocketAddress::getPort));
        
        for (int i = 0; i < sorted.size(); i++) {
            if (sorted.get(i).getPort() == localPort) return i;
        }
        return 0; 
    }

    /**
     * Executa o laço principal da thread, anunciando a presença do nó local
     * e escutando a entrada de novos membros no grupo multicast.
     */
    public void run() {
        new Thread(() -> {
            while (true) {
                try {
                    String msg = "PING:" + localPort;
                    DatagramPacket p = new DatagramPacket(msg.getBytes(), msg.length(), groupAddress, MULTICAST_PORT);
                    multicastSocket.send(p);
                    Thread.sleep(3000);
                } catch (Exception e) {}
            }
        }).start();

        byte[] buffer = new byte[256];
        while (true) {
            try {
                DatagramPacket p = new DatagramPacket(buffer, buffer.length);
                multicastSocket.receive(p);
                String msg = new String(p.getData(), 0, p.getLength());
                if (msg.startsWith("PING:")) {
                    int port = Integer.parseInt(msg.split(":")[1]);
                    InetSocketAddress member = new InetSocketAddress(p.getAddress(), port);
                    if (!members.contains(member)) {
                        members.add(member);
                    }
                }
            } catch (Exception e) {}
        }
    }
}