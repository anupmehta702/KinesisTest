import java.nio.ByteBuffer;

public class Application {
    public static void main(String[] args) {
        System.out.println("Welcome to Kinesis test. Run StockTradeReader and StockTradeWriter ");
        String k = "abcd";
        ByteBuffer b = ByteBuffer.wrap(k.getBytes());
        String v = new String(b.array());

        if(k.equals(v))
            System.out.println("it worked"+v);
        else
            System.out.println("did not work");
    }
}
