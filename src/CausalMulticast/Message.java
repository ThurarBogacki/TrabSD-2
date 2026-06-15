package CausalMulticast;

import java.io.Serializable;

public class Message implements Serializable {
    private static final long serialVersionUID = 1L;
    public String content;
    public int[][] vc; // Matriz de Relógios para Estabilização (Vetor de Vetores)
    public int senderId;

    public Message(String content, int[][] currentMatrix, int senderId) {
        this.content = content;
        this.senderId = senderId;
        this.vc = new int[currentMatrix.length][currentMatrix[0].length];

        for (int i = 0; i < currentMatrix.length; i++) {
            System.arraycopy(currentMatrix[i], 0, this.vc[i], 0, currentMatrix[i].length);
        }
    }
}