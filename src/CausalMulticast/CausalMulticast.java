package CausalMulticast;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class CausalMulticast {
    private int[][] mc;
    private final List<Message> buffer = new CopyOnWriteArrayList<>();
    private final List<DelayedUnicast> mensagensAtrasadas = new CopyOnWriteArrayList<>();
    private final DatagramSocket socket;
    private final ICausalMulticast client;
    private final DiscoveryService discovery;
    private int maxId = 10; 


    private static class DelayedUnicast {
        int id;
        Message message;
        InetSocketAddress target;

        DelayedUnicast(int id, Message message, InetSocketAddress target) {
            this.id = id;
            this.message = message;
            this.target = target;
        }
    }

    private int delayedCounter = 0;

    public CausalMulticast(String ip, Integer port, ICausalMulticast client) {
        this.client = client;
        this.mc = new int[maxId][maxId];
        try {
            this.socket = new DatagramSocket(port);
            this.discovery = new DiscoveryService(port);
            this.discovery.start();
            
            Thread.sleep(1500);
            
            startListening();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void mcsend(String msg, ICausalMulticast cliente) {

        if (msg.trim().toLowerCase().startsWith("liberar ")) {
        try {
            String[] partes = msg.trim().split(" ");
            int idAtraso = Integer.parseInt(partes[1]);
            
            System.out.println("[MIDDLEWARE] Comando detectado! Executando liberação local...");
            this.liberarMensagem(idAtraso); 
            
            return;
        } catch (Exception e) {
            System.out.println("[MIDDLEWARE] Erro ao processar comando de liberação interno.");
            return;
        }
    }

        int myId = discovery.getMyId();
        mc[myId][myId]++;
        
        Message message = new Message(msg, this.mc, myId);
        Scanner sc = new Scanner(System.in);
        
        List<InetSocketAddress> targets = new ArrayList<>(discovery.members);
        
        System.out.println("\n--- Controle de Transmissão Multicast ---");
        System.out.println("Enviar pacotes UNICAST para todos os " + targets.size() + " processos agora? (s/n)");
        
        if (sc.nextLine().equalsIgnoreCase("n")) {
            System.out.println("Selecione o índice do processo para ATRASAR o envio:");
            for (int i = 0; i < targets.size(); i++) {
                System.out.println("[" + i + "] -> " + targets.get(i));
            }
            System.out.print("Escolha o índice: ");
            int targetIdx = Integer.parseInt(sc.nextLine());
            
            for (int i = 0; i < targets.size(); i++) {
                if (i == targetIdx) {
                    int delayId = delayedCounter++;
                    mensagensAtrasadas.add(new DelayedUnicast(delayId, message, targets.get(i)));
                    System.out.println("[RETIDO] Envio para " + targets.get(i) + " armazenado com ID de liberação: " + delayId);
                } else {
                    enviarUnicast(message, targets.get(i));
                }
            }
        } else {
            for (InetSocketAddress target : targets) {
                enviarUnicast(message, target);
            }
        }
        printStatus();
    }

    public void liberarMensagem(int idAtraso) {
        for (DelayedUnicast du : mensagensAtrasadas) {
            if (du.id == idAtraso) {
                System.out.println("-> Liberando Unicast atrasado para: " + du.target);
                enviarUnicast(du.message, du.target);
                mensagensAtrasadas.remove(du);
                return;
            }
        }
        System.out.println("ID de atraso não localizado.");
    }

    private void enviarUnicast(Message message, InetSocketAddress target) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(message);
            byte[] data = baos.toByteArray();
            DatagramPacket packet = new DatagramPacket(data, data.length, target.getAddress(), target.getPort());
            socket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

private final Set<String> mensagensEntreguesNaAplicacao = Collections.synchronizedSet(new HashSet<>());

  private void startListening() {
    new Thread(() -> {
        byte[] receiveBuffer = new byte[8192];
        while (true) {
            try {
                DatagramPacket packet = new DatagramPacket(receiveBuffer, receiveBuffer.length);
                socket.receive(packet);
                
                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(packet.getData()));
                Message msg = (Message) ois.readObject();
                
                synchronized (this) {
                    buffer.add(msg);
                    
                    for (int k = 0; k < maxId; k++) {
                        this.mc[msg.senderId][k] = Math.max(this.mc[msg.senderId][k], msg.vc[msg.senderId][k]);
                    }
                    
                    processBuffer();
                    checkStabilization();
                    printStatus();
                }
            } catch (Exception e) {
            }
        }
    }).start();
}

private void processBuffer() {
    int myId = discovery.getMyId();
    boolean deliveredAny;
    do {
        deliveredAny = false;
        for (Message msg : buffer) {
            String msgId = msg.senderId + "_" + msg.vc[msg.senderId][msg.senderId];
            
            if (isCausallyOrdered(msg)) {
                buffer.remove(msg);

                if (!mensagensEntreguesNaAplicacao.contains(msgId)) {
                    mensagensEntreguesNaAplicacao.add(msgId);
                    
                    if (myId != msg.senderId) {
                        mc[myId][msg.senderId]++;
                    }
                
                    new Thread(() -> client.deliver(msg.content)).start();
                }
                
                deliveredAny = true;
                break; 
            }
        }
    } while (deliveredAny);
}

    private boolean isCausallyOrdered(Message msg) {
        int myId = discovery.getMyId();
        int sender = msg.senderId;

        if (sender == myId) return true;

        if (msg.vc[sender][sender] != mc[myId][sender] + 1) {
            return false;
        }
        for (int k = 0; k < maxId; k++) {
            if (k != sender && msg.vc[sender][k] > mc[myId][k]) {
                return false;
            }
        }
        return true;
    }

    private void checkStabilization() {
        int myId = discovery.getMyId();
        List<InetSocketAddress> activeMembers = discovery.members;
        
        buffer.removeIf(msg -> {
            int sender = msg.senderId;
            
            int minVisto = Integer.MAX_VALUE;
            for (int i = 0; i < activeMembers.size(); i++) {
                minVisto = Math.min(minVisto, mc[i][sender]);
            }
            
            boolean stable = msg.vc[sender][sender] <= minVisto;
            if (stable) {
                System.out.println(">> [ESTABILIZAÇÃO] Mensagem '" + msg.content + "' descartada com sucesso do buffer local.");
            }
            return stable;
        });
    }

    public void printStatus() {
        int myId = discovery.getMyId();
        System.out.println("\n=================================================");
        System.out.println(" STATUS DO PROCESSO LOCAL (ID Mapeado: " + myId + ")");
        System.out.println("=================================================");
        System.out.println("Matriz de Relógios Lógicos (Vetor de Vetores):");
        for (int i = 0; i < discovery.members.size(); i++) {
            System.out.println("  P" + i + ": " + Arrays.toString(Arrays.copyOf(mc[i], discovery.members.size())));
        }
        System.out.println("\nBuffer de Mensagens Ociosas: " + buffer.size());
        for (Message m : buffer) {
            System.out.println("  -> [Em espera] Origem: P" + m.senderId + " | Conteúdo: " + m.content);
        }
        if (!mensagensAtrasadas.isEmpty()) {
            System.out.println("\nUnicasts Retidos Localmente nesta Origem:");
            for (DelayedUnicast du : mensagensAtrasadas) {
                System.out.println("  -> [ID Liberação: " + du.id + "] Destinado a: " + du.target);
            }
        }
        System.out.println("=================================================\n");
    }
}