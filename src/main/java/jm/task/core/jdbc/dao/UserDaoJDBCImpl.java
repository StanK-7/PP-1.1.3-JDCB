package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {
    private final Util util = new Util();
    private Connection connection;

    public UserDaoJDBCImpl() {
    }

    public void createUsersTable() throws SQLException {
        Statement stmt = null;
        try {
            connection = util.getConnection();
            stmt = connection.createStatement();
            String sql = "CREATE TABLE if not exists `users` (" +
                    "`id` BIGINT(20) NOT NULL AUTO_INCREMENT, " +
                    "`name` VARCHAR(45) NOT NULL, " +
                    "`lastname` VARCHAR(45) NOT NULL, " +
                    "`age` TINYINT(3) NOT NULL, " +
                    "PRIMARY KEY (`id`));";
            stmt.execute(sql);
            connection.commit();

        } catch (SQLException e) {
            if (connection != null) {
                connection.rollback();
            }
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    public void dropUsersTable() throws SQLException {
        Statement stmt = null;
        try {
            connection = util.getConnection();
            stmt = connection.createStatement();
            String sql = "DROP TABLE IF EXISTS Users;";
            stmt.execute(sql);

        } catch (SQLException e) {
            if (connection != null) {
                connection.rollback();
            }
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    public void saveUser(String name, String lastName, byte age) throws SQLException {
        PreparedStatement stmt = null;
        try {
            connection = util.getConnection();
            String sql = "INSERT INTO users (name, lastname, age) VALUES (?, ?, ?)";
            stmt = connection.prepareStatement(sql);
            stmt.setString(1, name);
            stmt.setString(2, lastName);
            stmt.setByte(3, age);
            stmt.executeUpdate();
            connection.commit();

        } catch (SQLException e) {
            System.out.println("поймал исключение в saveUser");
            if (connection != null) {
                connection.rollback();
                e.printStackTrace();
            }
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (connection != null) {
                connection.close();
            }

        }
    }

    public void removeUserById(long id) throws SQLException {
//        PreparedStatement stmt = null;
        Statement stmt = null;

        try {
            connection = util.getConnection();
//            String sql = "DELETE FROM users WHERE id =?;";
//            stmt = connection.prepareStatement(sql);
//            stmt.setLong(1, id);
//            stmt.executeUpdate(sql);
            stmt = connection.createStatement();
            stmt.executeUpdate("DELETE FROM users WHERE id =" + id);
            connection.commit();
        } catch (SQLException e) {
            System.out.println("поймал исключение в removeUser");
            if (connection != null) {
                connection.rollback();
            }
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    public List<User> getAllUsers() throws SQLException {

        List<User> list = new ArrayList<>();
        Statement stmt = null;

        try {
            connection = util.getConnection();
            String sql = "SELECT ID, NAME, LASTNAME, AGE FROM users";
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                User user = new User();
                user.setId(rs.getLong(1));
                user.setName(rs.getString(2));
                user.setLastName(rs.getString(3));
                user.setAge(rs.getByte(4));

                list.add(user);
            }
            connection.commit();
        } catch (SQLException e) {
            System.out.println("поймал исключение в getAllUsers");
            if (connection != null) {
                connection.rollback();
            }
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        return list;
    }

    public void cleanUsersTable() throws SQLException {
        connection = util.getConnection();
        Statement stmt = null;
        try {
            connection = util.getConnection();
            String sql = "TRUNCATE users";
            stmt = connection.createStatement();
            stmt.executeUpdate(sql);
            connection.commit();
        } catch (SQLException e) {
            if (connection != null) {
                connection.rollback();
            }
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }
}
