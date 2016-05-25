package nhs.genetics.cardiff;

/**
 * A exception class for identifying nodes with unexpected labels
 *
 * @author  Matt Lyon
 * @version 1.0
 * @since   2016-02-16
 */
public class WrongLabelException extends RuntimeException {

    public WrongLabelException(String message) {
        super(message);
    }
    public String getMessage()
    {
        return super.getMessage();
    }

}
