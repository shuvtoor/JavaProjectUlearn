import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.util.stream.Collectors;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;


public class Main {
    public static void main(String[] args) {
        try {
            var people = getPeople();
            var connection = getConnection();
            createDB(people, connection);
            Parser.run(connection);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static ArrayList<Person> getPeople() throws IOException, CsvException {
        ArrayList<Person> people;
        var csvreader = new CSVReader(new FileReader("Forbes.csv"));

        csvreader.readNext();

        people = csvreader.readAll().stream().map(str ->
                new Person(Integer.parseInt(str[0]), str[1],
                        Double.parseDouble(str[2]),
                        Integer.parseInt(str[3]), str[4], str[5], str[6]))
                .collect(Collectors.toCollection(ArrayList::new));
        return people;
    }

    private static Connection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName("org.sqlite.JDBC");
        return DriverManager.getConnection("jdbc:sqlite:forbes.db");
    }

    private static void createDB(ArrayList<Person> people, Connection connection) throws SQLException {
        var statement = connection.createStatement();

        statement.execute("drop table 'forbes';");
        statement.execute(
                "CREATE TABLE 'forbes' ("
                        + "'rank' int,"
                        + "'name' varchar,"
                        + "'networth' real,"
                        + "'age' int,"
                        + "'country' text,"
                        + "'source' text,"
                        + "'industry' text);"
        );

        var prepareState = connection.prepareStatement(
                "INSERT INTO 'forbes' ("
                        + "'rank',"
                        + "'name',"
                        + "'networth',"
                        + "'age',"
                        + "'country',"
                        + "'source',"
                        + "'industry') VALUES (?,?,?,?,?,?,?);"
        );

        for (var p : people) {
            prepareState.setInt(1, p.getRank());
            prepareState.setString(2, p.getName());
            prepareState.setDouble(3, p.getNetworth());
            prepareState.setInt(4, p.getAge());
            prepareState.setString(5, p.getCountry());
            prepareState.setString(6, p.getSource());
            prepareState.setString(7, p.getIndustry());
            prepareState.executeUpdate();
        }
    }
}
