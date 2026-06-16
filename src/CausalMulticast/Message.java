package CausalMulticast;

import java.io.Serializable;

/**
 * Representa o pacote de dados que trafega na rede via UDP.
 * Esta classe encapsula o conteúdo da mensagem e as estruturas
 * necessárias para a ordenação causal e estabilização (Relógios Lógicos).
 * * @author Arthur Veríssimo
 * @author Davi de Castro Machado
 * @version 1.0
 */
public class Message implements Serializable {
    private static final long serialVersionUID = 1L;
    
    /** O conteúdo textual da mensagem enviada pela aplicação. */
    public String content;
    
    /** Matriz de Relógios Lógicos (Vetor de Vetores) para avaliação causal no destino. */
    public int[][] vc; 
    
    /** Identificador numérico do processo remetente. */
    public int senderId;

    /**
     * Instancia um novo pacote de mensagem capturando o estado do relógio local.
     * @param content O texto da mensagem.
     * @param currentMatrix A matriz de relógios atual do remetente.
     * @param senderId O ID numérico do remetente na rede.
     */
    public Message(String content, int[][] currentMatrix, int senderId) {
        this.content = content;
        this.senderId = senderId;
        this.vc = new int[currentMatrix.length][currentMatrix[0].length];

        for (int i = 0; i < currentMatrix.length; i++) {
            System.arraycopy(currentMatrix[i], 0, this.vc[i], 0, currentMatrix[i].length);
        }
    }
}