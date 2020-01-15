package at.htl.gotjdbcrepository.control;

import at.htl.gotjdbcrepository.entity.Person;
import org.apache.derby.client.am.SqlException;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class PersonRepository implements Repository {
    public static final String DRIVER_STRING = "org.apache.derby.jdbc.ClientDriver";
    public static final String USERNAME = "app";
    public static final String PASSWORD = "app";
    public static final String DATABASE = "db";
    public static final String URL = "jdbc:derby://localhost:1527/" + DATABASE + ";create=true";
    public static final String TABLE_NAME = "person";

    private static PersonRepository instance = null;
    Connection connection;

    private PersonRepository() {
        connection = null;
        try {
            Class.forName(DRIVER_STRING);
            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            connection.setAutoCommit(true);
        } catch (ClassNotFoundException ex) {
            System.out.println("Treiber laden nicht moeglich " + ex + "\n");
            System.exit(1);
        } catch (SQLException ex) {
            System.out.println("Verbindung zur Datenbank nicht moeglich " + ex + "\n");
            System.exit(1);
        }
    }

    public static synchronized PersonRepository getInstance() {
        if(instance == null) {
            instance = new PersonRepository();
            instance.createTable();
        }
        return instance;
    }

    private void createTable() {
        try (Connection conn = DriverManager.getConnection(URL, USERNAME, PASSWORD)) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "CREATE TABLE " + TABLE_NAME + " (" +
                        "id INT NOT NULL GENERATED ALWAYS AS IDENTITY CONSTRAINT " + TABLE_NAME + "_pk PRIMARY KEY," +
                        "name VARCHAR(255)," +
                        "city VARCHAR(255)," +
                        "house VARCHAR(255)," +
                        "CONSTRAINT " + TABLE_NAME + "_uq UNIQUE (name, city, house)" +
                        ")";
                stmt.executeUpdate(sql);
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            //System.err.format("SQL State: %s - %s\n", e.getSQLState(), e.getMessage());
        }
    }

    public void deleteAll() throws SQLException {
        PreparedStatement deleteAllStatement = connection.prepareStatement(
                "DELETE FROM " + TABLE_NAME
        );
        deleteAllStatement.executeUpdate();
    }

    /**
     *
     * Hat newPerson eine ID (id != null) so in der Tabelle die entsprechende Person gesucht und upgedated
     * Hat newPerson keine ID wird ein neuer Datensatz eingefügt.
     *
     * Wie man die generierte ID zurück erhält: https://stackoverflow.com/a/1915197
     *
     * Falls ein Fehler auftritt, wird nur die Fehlermeldung ausgegeben, der Programmlauf nicht abgebrochen
     *
     * Verwenden sie hier die privaten MEthoden update() und insert()
     *
     * @param newPerson
     * @return die gespeicherte Person mit der (neuen) id
     */
    @Override
    public Person save(Person newPerson) throws SQLException {

        return insert(newPerson);
    }

    /**
     *
     * Wie man die generierte ID erhält: https://stackoverflow.com/a/1915197
     *
     * @param personToSave
     * @return Rückgabe der Person inklusive der neu generierten ID
     */
    private Person insert(Person personToSave) throws SQLException {
        try (
                Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
                PreparedStatement statement = connection.prepareStatement("INSERT INTO APP.PERSON (name, city, house) values (?,?,?)",
                        Statement.RETURN_GENERATED_KEYS);
        ) {
           statement.setString(1, personToSave.getName());
           statement.setString(2, personToSave.getCity());
           statement.setString(3, personToSave.getHouse());

           int affectedRows = statement.executeUpdate();

           if (affectedRows == 0) {
               throw new SQLException("Creating Person failed, no rows affected");
           }

           try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
               if (generatedKeys.next()) {
                   personToSave.setId(generatedKeys.getLong(1));
               }
               else {
                   throw new SQLException("Creating Person failed, no ID obtained");
               }
           }
        }

        catch (SQLException e) {
            e.printStackTrace();
        }

        return personToSave;
    }

    /**
     *
     * @param personToSave
     * @return wenn erfolgreich --> Anzahl der eingefügten Zeilen, also 1
     *         wenn nicht erfolgreich --> -1
     */
    private int update(Person personToSave) {

        return -1;
    }

   // public void create(Person )

    @Override
    public void delete(long id) {

    }

    /**
     *
     * Finden Sie eine Person anhand Ihrer ID
     *
     * @param id
     * @return die gefundene Person oder wenn nicht gefunden wird null zurückgegeben
     */
    public Person find(long id) {
        Person foundPerson = new Person();
        try (
            Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            PreparedStatement statement = connection.prepareStatement("SELECT * FROM APP.PERSON where id = ?");
        ) {
        statement.setString(1, String.valueOf(id));
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            foundPerson = new Person(rs.getString(2), rs.getString(3), rs.getString(4));
            foundPerson.setId(rs.getLong(1));
        }
        statement.executeUpdate();

    } catch (SQLException e) {
        e.printStackTrace();
    }

        return foundPerson;
    }

    /**
     *
     * @param house Name des Hauses
     * @return Liste aller Personen des gegebenen Hauses
     */
    public List<Person> findByHouse(String house) {

        return null;
    }


}
