package nhs.genetics.cardiff;

/**
 * Created by ml on 16/02/2016.
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
