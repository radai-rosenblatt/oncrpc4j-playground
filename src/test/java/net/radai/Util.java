package net.radai;

/**
 * @author Radai Rosenblatt
 */
public class Util {
    public static Throwable getRootCause(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }
}
