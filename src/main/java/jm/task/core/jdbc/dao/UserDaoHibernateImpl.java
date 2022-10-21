package jm.task.core.jdbc.dao;


import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import org.hibernate.Session;
import org.hibernate.Transaction;

import java.util.List;

public class UserDaoHibernateImpl implements UserDao {


    public UserDaoHibernateImpl() {
    }

    @Override
    public void createUsersTable() {
        Transaction transaction = null;
        Session session= null;
        try {
            session = Util.getSessionFactory().getCurrentSession();
            session.beginTransaction();

            session.createSQLQuery("CREATE TABLE if not exists users (" +
                    "id BIGINT(20) NOT NULL AUTO_INCREMENT, " +
                    "name VARCHAR(45) NOT NULL, " +
                    "lastname VARCHAR(45) NOT NULL, " +
                    "age TINYINT(3) NOT NULL, " +
                    "PRIMARY KEY (id))").executeUpdate();

            transaction = session.getTransaction();
            transaction.commit();
            System.out.println("Success!");

        } catch (Exception e) {
            System.out.println("Transaction failed");
            if (transaction != null) {
                transaction.rollback();
            }
            e.printStackTrace();
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    @Override
    public void dropUsersTable() {
        Transaction transaction = null;
        Session session = null;
        try {
            session = Util.getSessionFactory().getCurrentSession();
            session.beginTransaction();

            session.createSQLQuery("DROP TABLE IF EXISTS Users")
                    .executeUpdate();

            transaction = session.getTransaction();
            transaction.commit();
            System.out.println("Success!");

        } catch (Exception e) {
            System.out.println("Transaction failed");
            if (transaction != null) {
                transaction.rollback();
            }
            e.printStackTrace();
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }


    @Override
    public void saveUser(String name, String lastName, byte age) {
        Transaction transaction = null;
        Session session = null;
        try {
            session = Util.getSessionFactory().getCurrentSession();
            User user = new User(name, lastName, age);
            transaction = session.beginTransaction();
            session.save(user);
//            transaction = session.getTransaction();
            transaction.commit();
            System.out.println("Success!");

        } catch (Exception e) {
            System.out.println("Transaction failed");
            if (transaction != null) {
                transaction.rollback();
            }
            e.printStackTrace();
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    @Override
    public void removeUserById(long id) {
        Transaction transaction = null;
        Session session = null;
        try {
            session = Util.getSessionFactory().getCurrentSession();
            transaction = session.beginTransaction();

            User user = session.get(User.class, id);
            session.delete(user);

//            session.createQuery("delete User where id = :id").executeUpdate();

            transaction.commit();
            System.out.println("Success!");
        } catch (Exception e) {
            System.out.println("Transaction failed");
            if (transaction != null) {
                transaction.rollback();
            }
            e.printStackTrace();
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    @Override
    public List<User> getAllUsers() {
        Transaction transaction = null;
        Session session = null;
        List<User> users = null;
        try {
            session = Util.getSessionFactory().getCurrentSession();
            session.beginTransaction();

            users = session.createQuery("from User")
                    .getResultList();

            for (User u : users
            ) {
                System.out.println(u);
            }
            transaction = session.getTransaction();
            transaction.commit();
            System.out.println("Success!");
        } catch (Exception e) {
            System.out.println("Transaction failed");
            if (transaction != null) {
                transaction.rollback();
            }
            e.printStackTrace();
        } finally {
            if (session != null) {
                session.close();
            }
        }
        return users;
    }

    @Override
    public void cleanUsersTable() {
        Transaction transaction = null;
        Session session = null;
        try {
            session = Util.getSessionFactory().getCurrentSession();
            session.beginTransaction();
            session.createQuery("delete User ").executeUpdate();
            transaction = session.getTransaction();
            transaction.commit();
            System.out.println("Success!");
        } catch (Exception e) {
            System.out.println("Transaction failed");
            if (transaction != null) {
                transaction.rollback();
            }
            e.printStackTrace();
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }
}
