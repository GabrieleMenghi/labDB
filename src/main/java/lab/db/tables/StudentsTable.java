package lab.db.tables;

 import java.sql.Connection;
 import java.sql.PreparedStatement;
 import java.sql.ResultSet;
 import java.sql.Statement;
 import java.sql.SQLException;
 import java.sql.SQLIntegrityConstraintViolationException;
 import java.util.ArrayList;
 import java.util.Date;
 import java.util.List;
 import java.util.Objects;
 import java.util.Optional;

 import lab.utils.Utils;
 import lab.db.Table;
 import lab.model.Student;

 public final class StudentsTable implements Table<Student, Integer> {    
     public static final String TABLE_NAME = "students";

     private final Connection connection; 
     

     public StudentsTable(final Connection connection) {
         this.connection = Objects.requireNonNull(connection);
     }

     @Override
     public String getTableName() {
         return TABLE_NAME;
     }

     @Override
     public boolean createTable() {
         // 1. Create the statement from the open connection inside a try-with-resources
         try (final Statement statement = this.connection.createStatement()) {
             // 2. Execute the statement with the given query
             statement.executeUpdate(
                 "CREATE TABLE " + TABLE_NAME + " (" +
                         "id INT NOT NULL PRIMARY KEY," +
                         "firstName CHAR(40)," + 
                         "lastName CHAR(40)," + 
                         "birthday DATE" + 
                     ")");
             return true;
         } catch (final SQLException e) {
             // 3. Handle possible SQLExceptions
             return false;
         }
     }

     @Override
     public Optional<Student> findByPrimaryKey(final Integer id) {
    	 final String query = "SELECT * FROM " + TABLE_NAME +  " WHERE id = ?";
    	 try(final PreparedStatement statement = this.connection.prepareStatement(query)){
    		 statement.setInt(1, id);
    		 /*final ResultSet rs = statement.executeQuery(
					"SELECT * FROM" + TABLE_NAME + "WHERE id = " + id   SQL injection
				);*/
    		 final ResultSet rs = statement.executeQuery();
    		 return readStudentsFromResultSet(rs).stream().findFirst();
    	 } catch (final SQLException e) {
    		 return Optional.empty();
    	 }
     }

     /**
      * Given a ResultSet read all the students in it and collects them in a List
      * @param resultSet a ResultSet from which the Student(s) will be extracted
      * @return a List of all the students in the ResultSet
      */
     private List<Student> readStudentsFromResultSet(final ResultSet resultSet) {
         // Create an empty list, then
         // Inside a loop you should:
         //      1. Call resultSet.next() to advance the pointer and check there are still rows to fetch
         //      2. Use the getter methods to get the value of the columns
         //      3. After retrieving all the data create a Student object
         //      4. Put the student in the List
         // Then return the list with all the found students

         // Helpful resources:
         // https://docs.oracle.com/javase/7/docs/api/java/sql/ResultSet.html
         // https://docs.oracle.com/javase/tutorial/jdbc/basics/retrieving.html
         final List<Student> studentsList = new ArrayList<>();
         try {
			while(resultSet.next()) {
				 studentsList.add(new Student(resultSet.getInt(1), resultSet.getString(2), resultSet.getString(3)));
			 }
		} catch (SQLException e) {}
         return studentsList;
     }

     @Override
     public List<Student> findAll() {
         throw new UnsupportedOperationException("TODO");
     }

     public List<Student> findByBirthday(final Date date) {
         throw new UnsupportedOperationException("TODO");
     }

     @Override
     public boolean dropTable() {
         throw new UnsupportedOperationException("TODO");
     }

     @Override
     public boolean save(final Student student) {
    	 final String q = "SELECT * FROM " + TABLE_NAME +
    			 " WHERE id = ?";
    	 try(final PreparedStatement st = this.connection.prepareStatement(q)){
    		 st.setInt(1, student.getId());
    		 final ResultSet res = st.executeQuery();
    		 if(res.getRow() == 0) {
    			 final String query = "INSERT into " + TABLE_NAME + " (id, firstName, lastName, bithday)" 
	 						+ " VALUES (?, ?, ?, ?)";
    			 try(final PreparedStatement statement = this.connection.prepareStatement(query)){
    				 statement.setInt(1, student.getId());
    				 statement.setString(2, student.getFirstName());
    				 statement.setString(3, student.getLastName());
    				 statement.setDate(4, Utils.dateToSqlDate(student.getBirthday().get()));
    				 final ResultSet rs = statement.executeQuery();
    				 return true;
    			 } catch (final SQLException e) {
    				 return false;
    			 }
    		 }
    	 } catch (final SQLException e) {}
    	 return false;
    	 
     }

     @Override
     public boolean delete(final Integer id) {
         throw new UnsupportedOperationException("TODO");
     }

     @Override
     public boolean update(final Student student) {
         throw new UnsupportedOperationException("TODO");
     }
 }