using System;
using System.Configuration;
using System.Collections.Generic;
using System.Text;
using System.Threading;

using System.Data;
using System.Data.Common;
using System.Transactions;
using Oracle.DataAccess.Client;


namespace TTAdoTest
{
  using NUnit.Framework;


  [TestFixture]
  public class ConnTest : TestBase
  {


    [Test]
    public void TestTxnManagement()
    {
      OracleCommand cmd = Conn.CreateCommand();
      OracleTransaction trans;

      /* instantiate a second connection, command and transaction */
      OracleConnection conn2 = (OracleConnection)ProviderFactory.CreateConnection();
      OracleCommand cmd2 = (OracleCommand)ProviderFactory.CreateCommand();
      OracleTransaction trans2;

      cmd2.Connection = conn2;
      conn2.ConnectionString = ConnStr;
      conn2.Open();

      trans = conn2.BeginTransaction(System.Data.IsolationLevel.Serializable);

      try
      {
        cmd.CommandText = "DROP TABLE TXN_MGM";
        cmd.ExecuteNonQuery();
      }
      catch (OracleException) { }


      // verify that commands that are not
      // associated with transactions behave as if ODBC autocommit mode is on
      cmd.CommandText = "CREATE TABLE TXN_MGM (C1 INT PRIMARY KEY)";
      cmd.ExecuteNonQuery();

      // can this insert be seen by other connections even though the
      // transaction has not been committed explicitly?
      cmd.CommandText = "INSERT INTO TXN_MGM VALUES (1)";
      cmd.ExecuteNonQuery();


      cmd2.CommandText = "SELECT COUNT (*) FROM TXN_MGM";

      if (Conn.ConnectionType == OracleConnectionType.TimesTen)
      {
        Console.WriteLine("SELECT COUNT (*) FROM TXN_MGM (conn2) = " + cmd2.ExecuteScalar());
        Assert.AreEqual("1", cmd2.ExecuteScalar().ToString());
      }

      trans.Commit();

      // explicitly initialize a serializable transaction
      trans = Conn.BeginTransaction(System.Data.IsolationLevel.Serializable);

      cmd.CommandText = "DELETE FROM TXN_MGM";
      cmd.ExecuteNonQuery();

      // the transaction has not been committed - the row should
      // still exist for other connections
      Console.WriteLine("SELECT COUNT (*) FROM TXN_MGM (conn2) = " + cmd2.ExecuteScalar());
      Assert.AreEqual("1", cmd2.ExecuteScalar().ToString());

      // commit the txn and verify the delete from another connection
      trans.Commit();
      trans.Dispose();

      Console.WriteLine("SELECT COUNT (*) FROM TXN_MGM (conn2) = " + cmd2.ExecuteScalar());
      Assert.AreEqual("0", cmd2.ExecuteScalar().ToString());

      // verify transaction timeouts and automatic rollbacks
      trans2 = conn2.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
      trans = Conn.BeginTransaction(System.Data.IsolationLevel.Serializable);

      cmd2.CommandText = "INSERT INTO TXN_MGM VALUES (1)";
      cmd2.ExecuteNonQuery();

      // this should time out 
      if (Conn.ConnectionType == OracleConnectionType.TimesTen)
      {
        try
        {
          cmd.CommandText = "INSERT INTO TXN_MGM VALUES (1)";
          cmd.ExecuteNonQuery();
        }
        catch (OracleException ex)
        {
          Console.WriteLine("OracleException: " + ex);
        }
      }

      // close a connection with an open transaction and then verify
      // that the transaction is rolled back automatically (via the
      // connection pool)
      conn2.Close();

      // this should not time out
      cmd.ExecuteNonQuery();
      trans.Commit();

      cmd.CommandText = "SELECT COUNT (*) FROM TXN_MGM";
      Console.WriteLine("SELECT COUNT (*) FROM TXN_MGM = " + cmd.ExecuteScalar());

      if (Conn.ConnectionType == OracleConnectionType.TimesTen)
        Assert.AreEqual("1", cmd.ExecuteScalar().ToString());
      else
        Assert.AreEqual("0", cmd.ExecuteScalar().ToString());

      // cleanup
      cmd.CommandText = "DROP TABLE TXN_MGM";
      cmd.ExecuteNonQuery();

      return;
    }


    // This test case attempts to enlist two connections within a distributed
    // transaction.

    [Test]
    public void TestDistributedTxn()
    {

      try
      {
        // set a system transaction scope 
        CommittableTransaction txn = new CommittableTransaction();

        // open a new connection 
        DbConnection conn = ProviderFactory.CreateConnection();
        conn.ConnectionString = ConnStr + ";Promotable Transaction=promotable;Enlist=true";
        conn.Open();

        conn.EnlistTransaction(txn);

        // execute some statements 
        DbCommand cmd = conn.CreateCommand();
        cmd.Connection = conn;

        // setup the test table
        try
        {
          cmd.CommandText = "DROP TABLE EMP";
          cmd.ExecuteNonQuery();
        }
        catch { };

        cmd.CommandText = "CREATE TABLE EMP (EMPNO NUMBER, ENAME VARCHAR (20), JOB VARCHAR (20))";
        cmd.ExecuteNonQuery();

        cmd.CommandText = @"insert into emp (empno, ename, job) values (1234, 'emp1', 'dev1')";
        Console.WriteLine("Rows affected by cmd: {0}", cmd.ExecuteNonQuery());

        // create a second connection object
        DbConnection conn2 = ProviderFactory.CreateConnection();
        conn2.ConnectionString = ConnStr + ";Promotable Transaction=promotable;Enlist=true";
        conn2.Open();

        conn2.EnlistTransaction(txn);

        // execute some statements
        DbCommand cmd2 = conn2.CreateCommand();
        cmd2.CommandText = @"insert into emp (empno, ename, job) values (1234, 'emp1', 'dev1')";
        Console.WriteLine("Rows affected by cmd: {0}", cmd2.ExecuteNonQuery());


        // commit the distributed transaction
        txn.Commit();


        // Close the second connection and dispose the command object.
        conn2.Close();
        conn2.Dispose();
        cmd2.Dispose();

        // Close the first connection and dispose the command object.
        conn.Close();
        conn.Dispose();
        cmd.Dispose();

      }
      catch (Exception ex)
      {
        Console.WriteLine(ex.Message);
        Console.WriteLine(ex.StackTrace);
      }
    }


    [Test]
    public void TestConnPoolRecovery()
    {
      OracleCommand cmd = Conn.CreateCommand();
      bool validateConnection = true;


      // is the ValidateConnection attribute set to true?
      String connStr = Conn.ConnectionString.ToUpper();

      if (!connStr.Contains("VALIDATE CONNECTION=TRUE"))
      {
        validateConnection = false;
        Console.WriteLine("Validate Connection=false");
      }
      else
      {
        Console.WriteLine("Validate Connection=true");
      }


      // invalidate the connection
      try
      {
        cmd.CommandText = "CALL Invalidate ()";
        cmd.ExecuteNonQuery();
      }
      catch (OracleException ex)
      {
        Console.WriteLine("OracleException: " + ex);
      }


      // the connection should be in the 'Closed' state after the invalidation
      // Console.WriteLine("Connection state: " + Conn.State);
      // Assert.AreEqual("Closed", Conn.State.ToString().Trim());

      // close the invalid connection again and verify the state
      Conn.Close();
      Console.WriteLine("Connection state: " + Conn.State);
      Assert.AreEqual("Closed", Conn.State.ToString().Trim());


      // if ValidateConnection=false then an invalidation will not cause
      // the connection pool to recover automatically, in this case
      // the application has to explicitly invalidate the pool
      if (!validateConnection)
      {
        Console.WriteLine("Invalidating connection pool...");
        OracleConnection.ClearPool(Conn);
      }


      // open a new connection - this should succeed and it should not 
      // return an invalid connection
      OracleConnection conn = (OracleConnection)ProviderFactory.CreateConnection();
      conn.ConnectionString = ConnStr;

      Console.WriteLine("Trying to recover...");
      conn.Open();
      Console.WriteLine("Connection state: " + Conn.State);

      cmd = conn.CreateCommand();
      cmd.CommandText = "SELECT * FROM DUAL";
      String dummy = (String) cmd.ExecuteScalar();
      Console.WriteLine("SELECT * FROM DUAL: " + dummy);

      conn.Close();
      Console.WriteLine("Connection state: " + Conn.State);


      // reopen the TestBase connection
      Conn.Open();

      return;
    }


    // This is a multi-threaded connect/disconnect test without a connection 
    // pool 
    private class ConnThread 
    {
      public int numIterationsPerThread = 1;
      OracleConnection conn;
      String connStr;

      public ConnThread(String connStr)
      {
        this.connStr = connStr;
      }

      public void run()
      {
        for (int iteration = 0; iteration < numIterationsPerThread; iteration++)
        {
          conn = new OracleConnection(connStr);
          conn.Open();

          OracleCommand cmd = conn.CreateCommand();
          cmd.CommandText = "SELECT CAST (SYSDATE AS VARCHAR (32)) FROM DUAL;";
          Console.WriteLine("Thread #" + Thread.CurrentThread.GetHashCode () + ": " + 
            (String)cmd.ExecuteScalar());

          conn.Close();
          conn.Dispose();
        }

      }
    }

    [Test]
    public void TestMultiThread()
    {
      Int32 numThreads = 1;
      Int32 numIterationsPerThread = 1;

      AppSettingsReader reader = new AppSettingsReader();
      numThreads = (Int32)reader.GetValue("ThreadCount", numThreads.GetType());
      numIterationsPerThread = (Int32)reader.GetValue("IterationsPerThread", numIterationsPerThread.GetType());

      Thread[] threads = new Thread[numThreads];
      ConnThread[] connThreads = new ConnThread[numIterationsPerThread];



      // clear the existing pool 
      OracleConnection.ClearPool(Conn);

      // removing any existing Pooling property from the conn. str.
      OracleConnectionStringBuilder connStrBuild = new OracleConnectionStringBuilder(ConnStr);
      connStrBuild.Remove("Pooling");
      connStrBuild.Add("Pooling", "false");

      Console.WriteLine("Using connection string: " + connStrBuild.ToString());
      Console.WriteLine("Spawning connection threads...");

      for (int i = 0; i < numThreads; i++)
      {
        Console.WriteLine("Creating thread #" + i + "...");
        connThreads[i] = new ConnThread(connStrBuild.ToString());
        connThreads[i].numIterationsPerThread = numIterationsPerThread;
        threads[i] = new Thread(new ThreadStart(connThreads[i].run));
      }

      for (int i = 0; i < numThreads; i++)
      {
        threads[i].Start();
      }

      Console.WriteLine("All threads have started.");
      Console.WriteLine("Running...");

      for (int i = 0; i < numThreads; i++)
      {
        threads[i].Join();
      }

      Console.WriteLine("All threads have completed.");

    }


    [Test]
    public void TestConnOpenError()
    {

      // Create a conn. string with a bogus user.
      String connStr = "Data Source=" + Conn.DataSource + 
        ";User ID=nobody";

      // Try to connect.
      try
      {
        Console.WriteLine("Connecting to {0} ...", connStr);
        OracleConnection conn = new OracleConnection(connStr);
        conn.Open();
      }
      catch (Oracle.DataAccess.Client.OracleException ex)
      {
        Console.WriteLine("OracleException: {0}", ex.Message);

        if (!ex.Message.Contains("TT7001") && !ex.Message.Contains("ORA-1017") &&
          !ex.Message.Contains("ORA-01017"))
        {
          throw new Exception ("Unexpected OracleException.");
        }
      }


      return;
    }

  }
}