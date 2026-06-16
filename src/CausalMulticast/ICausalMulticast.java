package CausalMulticast;

/**
 * Contrato que a aplicação principal deve implementar para receber mensagens do middleware.
 */
public interface ICausalMulticast {
    /**
     * Entrega uma mensagem causalmente ordenada à aplicação.
     * @param msg O conteúdo da mensagem pronta para ser exibida.
     */
    public void deliver(String msg);
}