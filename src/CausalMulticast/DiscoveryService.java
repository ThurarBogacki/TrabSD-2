package CausalMulticast;

import java.net.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class DiscoveryService extends Thread {
    private int localPort;
    public List<InetSocketAddress> members = new CopyOnWriteArrayList<>();
    private MulticastSocket multicastSocket;
    private InetAddress groupAddress;
    private static final String MULTICAST_IP = "224.0.0.1";
    private static final int MULTICAST_PORT = 8888;

    public DiscoveryService(int localPort) {
        this.localPort = localPort;
        try {
            this.multicastSocket = new MulticastSocket(MULTICAST_PORT);
            this.groupAddress = InetAddress.getByName(MULTICAST_IP);
            this.multicastSocket.joinGroup(groupAddress);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int getMyId() {
        // Ordena os membros pelo IP/Porta para mapear um ID determinístico (0, 1, 2...)
        List<InetSocketAddress> sorted = new ArrayList<>(members);
        sorted.sort(Comparator.comparing((InetSocketAddress addr) -> addr.getAddress().getHostAddress())
                              .thenComparingInt(InetSocketAddress::getPort));
        
        for (int i = 0; i < sorted.size(); i++) {
            if (sorted.get(i).getPort() == localPort) return i;
        }
        return 0; // Fallback
    }

    public void run() {
        // Thread para anunciar presença periodicamente
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

        // Ouvir anúncios de novos membros
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