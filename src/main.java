import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;


// Customer data headers: ID, Name, Age, Gender, Country Code, Salary
// Transaction data headers: ID, Total, NumItems, Description

public class main {

    public static void main(String[] args) throws IOException {
        System.out.println("Hello, world!");
        generateCustomerData();
        generateTransactionData();
    }

    public static void generateCustomerData() throws IOException {
        File csvFile = new File("data/customers.csv");
        FileWriter fileWriter = new FileWriter(csvFile);

        // Random number generator object
        Random rand = new Random();

        for(int i = 0; i < 50000; i++){

            // For each customer, generate some fake data
            int ID = i + 1;
            String name = getSaltString(20); // Do project instructions imply random length as well as random characters?
            int age = rand.nextInt(60) + 10 + 1;
            String gender = rand.nextInt(2) == 0 ? "Male" : "Female";
            int countryCode = rand.nextInt(10) + 1;
            float salary = (rand.nextFloat() * 9900) + 100;

            // Concat all the info into one line
            String csvLine = String.format("%s,%s,%s,%s,%s,%s\n", ID, name, age, gender, countryCode, salary);

            // Write to csv
            fileWriter.write(csvLine);

        }

        fileWriter.close();

    }

    public static void generateTransactionData() throws IOException {
        File csvFile = new File("data/transactions.csv");
        FileWriter fileWriter = new FileWriter(csvFile);

        // Random number generator object
        Random rand = new Random();

        for(int i = 0; i < 5000000; i++){
            int transID = i + 1;
            int custID = rand.nextInt(50000) + 1;
            float transTotal = (rand.nextFloat() * 990) + 10;
            int transNumItems = rand.nextInt(10) + 1;
            String transDesc = getSaltString(50);

            // Concat all the info into one line
            String csvLine = String.format("%s,%s,%s,%s,%s\n", transID, custID, transTotal, transNumItems, transDesc);

            // Write to csv
            fileWriter.write(csvLine);
        }

        fileWriter.close();

    }

    public static String getSaltString(int len) {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < len) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;

    }
}
