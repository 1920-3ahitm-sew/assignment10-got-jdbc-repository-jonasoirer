package at.htl.gotjdbcrepository.control;

import at.htl.gotjdbcrepository.entity.Person;

import java.sql.SQLException;

public interface Repository {
    public Person save(Person p) throws SQLException;
    public void delete(long id);
}
