package cn.xhjava.hoodie.callback.domain;

public class ClientOutput {
    private String text;
    private int exitCode = -1;

    /**
     * @param text
     * @param exitCode
     */
    public ClientOutput(int exitCode, String text) {
        this.text = text;
        this.exitCode = exitCode;
    }

    /**
     * @return the text
     */
    public String getText() {
        return text.toString();
    }

    /**
     * @return the exitCode
     */
    public int getExitCode() {
        return exitCode;
    }

}
