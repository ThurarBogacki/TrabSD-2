package CausalMulticast;

import java.util.Scanner;

/**
 * Classe principal da aplicação. Fornece a interface de terminal (CLI) para o usuário.
 */
public class Main implements ICausalMulticast {

    /**
     * Construtor padrão da aplicação. 
     * Declarado explicitamente para fins de documentação do JavaDoc.
     */
    public Main() {
    }

    /**
     * Ponto de entrada do programa.
     * @param args Argumentos de linha de comando (espera receber a porta).
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: java CausalMulticast.Main <porta>");
            return;
        }

        int port = Integer.parseInt(args[0]);

        Main app = new Main();
        CausalMulticast middleware = new CausalMulticast("127.0.0.1", port, app);

        // Pausa para descoberta automática via Multicast
        try { Thread.sleep(2000); } catch (InterruptedException e) {}

        Scanner scanner = new Scanner(System.in);
        System.out.println("=== SISTEMA DE MULTICAST CAUSAL ===");
        System.out.println("Comandos: ");
        System.out.println(" - Digite qualquer texto para enviar uma mensagem.");
        System.out.println(" - 'liberar <id>' para enviar mensagem retida.");
        System.out.println(" - 'sair' para encerrar.");
        System.out.print("> ");
        
        while (true) {
            String input = scanner.nextLine();
            
            if (input.equalsIgnoreCase("sair")) {
                break;
            } else {
                middleware.mcsend(input, app);
            }
            System.out.print("> ");
        }
        scanner.close();
        System.exit(0);
    }

    @Override
    public void deliver(String msg) {
        System.out.println("\n>>> ENTREGA DA APLICAÇÃO: " + msg);
        System.out.print("> "); 
        System.out.flush();
    }
}