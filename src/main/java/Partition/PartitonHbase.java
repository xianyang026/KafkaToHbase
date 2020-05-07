package Partition;

public class PartitonHbase {
    public static String partitionHbase(Integer number, Integer length) {
        if (length == 1) {
            return "00" + number + "|";
        } else if (length == 2) {
            return "0" + number + "|";
        } else {
            return "0error|";
        }
    }
}
