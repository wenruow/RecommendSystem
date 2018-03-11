import java.io.IOException;

public class Driver {

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

        DataDividerByUser dataDividerByUser = new DataDividerByUser();
        CoOccurenceMatrixGenerator coOccurenceMatrixGenerator = new CoOccurenceMatrixGenerator();
        Normalize normalize = new Normalize();
        Multiplication multiplication = new Multiplication();
        Sum sum = new Sum();

        String rawInput = args[0];
        String dataDividedByUser = args[1];
        String coOccurenceMatrix = args[2];
        String normalized = args[3];
        String multiplicationResult = args[4];
        String sumResult = args[5];

        String[] pathOne = {rawInput, dataDividedByUser};
        String[] pathTwo = {dataDividedByUser, coOccurenceMatrix};
        String[] pathThree = {coOccurenceMatrix, normalized};
        String[] pathFour = {normalized, rawInput, multiplicationResult};
        String[] pathFive = {multiplicationResult, sumResult};

        dataDividerByUser.main(pathOne);
        coOccurenceMatrixGenerator.main(pathTwo);
        normalize.main(pathThree);
        multiplication.main(pathFour);
        sum.main(pathFive);
    }
}
