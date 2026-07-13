// TradeManager.java

// The TradeManager class is responsible for managing TradeThread objects, 
// coordinating the test process and reporting and verifying results.



// IMPORTS
import java.io.*;
import java.lang.*;
import java.util.*;
import java.sql.*;
import javax.sql.*;
import com.timesten.jdbc.*;



public class TradeManager
{
  
  // ATTRIBUTES
  
  // test input
  private String m_url;
  private boolean m_quiet;
  private int m_txns;

  private String m_txnCodes;
  private String m_txnProbs;

  private LinkedHashMap<String, Integer> m_txnMap;
  private LinkedHashMap<String, Integer> m_txnProbsMap;

  private int m_threadCount;
  private int m_cacheSize;
  private boolean m_initCache;
  private boolean m_convertToJson;
  private int m_poolSize;

  
  // data source objects
  private DataSource m_dataSource;
  private Connection m_conn [];
  
  // misc.
  private Random m_rand;
  private TTTimer m_timer;

  // the collection of dispatched TradeThread objects
  private Vector <TradeThread> m_threads;

  // the collection of table row counts
  private HashMap<String, Integer> m_rows;

  // execution times (ms) for all threads by procedure
  private Vector<Long> m_timings;
  private Vector<String> m_timingProcs;
  

  // data table cache
  HashMap<String, ArrayList<HashMap<String, Object>>> m_tableCache;


  // statistics output files
  private static String TRADE_TPS_FILE = "trade-tps.csv";
  private static String TRADE_RESPONSE_FILE = "trade-response.csv";

  

  // OPERATIONS
  
  // ----------------------------------------
  // TradeManager ()
  public TradeManager (String url, int txns, String txnCodes, String txnProbs,
                      int threads, int cacheSize, boolean initCache,
                      boolean convertToJson, boolean quiet)
    throws Exception
  {
    
    // copy the arguments to the class attributes
    m_url = url;
    m_txns = txns;
    m_threadCount = threads;
    m_cacheSize = cacheSize;
    m_initCache = initCache;
    m_convertToJson = convertToJson;
    m_poolSize = threads;
    m_quiet = quiet;
    
    // initialize other class attributes
    m_dataSource = null;
    
    m_threads = new Vector <TradeThread> ();
    m_rows = new HashMap<>();

    m_timings = new Vector<>(); 
    m_timingProcs = new Vector<>();
 
    m_timer = new TTTimer (); 
    m_rand = new Random ();
    
    m_tableCache = new HashMap<>();


    // the transaction and transaction probability lists
    m_txnCodes = txnCodes;
    m_txnProbs = txnProbs;


    /* load drivers */
    try
    {
      Class.forName ("com.timesten.jdbc.TimesTenDriver");
    }
    catch (java.lang.ClassNotFoundException e)
    {/* do nothing */}
    
    return;
  }
  
  
  
  // ----------------------------------------
  // run ()
  // Begin test procedure
  public boolean run ()
    throws Exception
  {
    // locals
    boolean status = true;
    
    // build the transaction and probability maps
    m_txnMap = getTxnMap (m_txnCodes);
    m_txnProbsMap = getTxnProbsMap (m_txnMap, m_txnProbs);  

    /*
    logForce ("TradeManager", "Transactions : " + m_txnMap);
    logForce ("TradeManager", "Probabilities: " + m_txnProbsMap); 
    */
    


    // initialize
    logForce ("TradeManager", "Running ..."); 

    buildConnectionPool ();
    createDataTables ();
    cacheDataTables ();
    fetchTableRowCounts ();



    // start the the threads ...
    dispatchThreads (m_threadCount);
    
    // start the timer
    m_timer.start ();

    // loop until all of the threads have finished executing
    while (areThreadsAlive ())
    {
      try 
      {
        Thread.sleep (1);
      }
      catch (InterruptedException e)
      {
        handleException ("TradeManager", e);
        status = false;
      }
    }

    // stop the timer
    m_timer.stop ();

    // has any TradeThread failed
    if (!didThreadsSucceed ())
    {
      status = false;
    }

    

    // report results 
    reportSummaryStats ();

    return status;
  }
  
  

  
  // ----------------------------------------
  // dispatchThreads ()
  // This method is responsible for creating and starting threads
  private boolean dispatchThreads (int numThreads)
  {
    // locals
    boolean status = true;
    TradeThread tradeThread = null;
    int index = 0;

    
    for (index = 0; index < numThreads; index ++)
    {
      // create the thread
      tradeThread = new TradeThread (this, index, m_txns, m_txnMap, m_txnProbsMap);
      
      // add the thread to the tradeThread Vector
      m_threads.add (tradeThread);
      
      // execute 
      tradeThread.start ();
    }
    


    // get an array of running threads
    TradeThread [] threads = new TradeThread [1];
    threads = (TradeThread[]) m_threads.toArray (threads);
    
    // check every thread for the ready state
    for (index = 0; index < threads.length; index ++)
    {
      if (!threads [index].getReady ())
      {
        // wait for the ready state
        while (!threads [index].getReady ())
        {
          try 
          {
            Thread.sleep (1);
          }
          catch (InterruptedException e)
          {
            handleException ("TradeManager", e);
            status = false;
          }
        }
      }
    }


    // start the threads
    for (index = 0; index < threads.length; index ++)
    {
      threads [index].setStart ();
    }

    return status;
  }
  

  // ----------------------------------------
  // fetchTableRowCounts ()
  // Iterates though all user tables and counts the rows, the
  // results are saved in m_rows
  private void fetchTableRowCounts () 
    throws SQLException
  {
    Connection conn = null;
    
    Statement stmtTables = null;
    Statement stmtRowCount = null;

    ResultSet rsTables = null;
    ResultSet rsRowCount = null;

    int rowCount = 0;
    String tableName = null;
    String selectQuery = null;

    log ("TradeManager", "Fetching table row counts ...");

    conn = checkoutConn ("TradeManager");

    stmtTables = conn.createStatement ();
    stmtRowCount = conn.createStatement ();

    rsTables = stmtTables.executeQuery 
      ("SELECT TABLE_NAME FROM USER_TABLES");

    while (rsTables.next ())
    {

      tableName = rsTables.getString (1);
      selectQuery = "SELECT COUNT (*) FROM " + tableName;

      rsRowCount = stmtRowCount.executeQuery (selectQuery); 

      rsRowCount.next ();
      rowCount = rsRowCount.getInt (1);
      rsRowCount.close ();

      m_rows.put (tableName, rowCount);
    }

    stmtRowCount.close ();
    stmtTables.close ();

    checkinConn ("TradeManager", conn);
    
    return;
  }


  // ----------------------------------------
  // getTableRowCount ()
  // Returns the number of rows in the given table
  public int getTableRowCount (String tableName) 
  {
    return m_rows.get (tableName);
  }


  // ----------------------------------------
  // createDataTables ()
  // Creates copies of the trade tables in the user's account
  private void createDataTables () 
    throws SQLException
  {

    log ("TradeManager", "Creating data tables ...");

    if (m_convertToJson)
    {
      convertToJson ("MARKET_QUOTES_RGN");
      convertToJson ("INTERNAL_QUOTES_RGN"); 
      convertToJson ("QUOTE_SUBSCRIPTIONS_RGN"); 
      convertToJson ("INTERNAL_QUOTE_SUBSCRIPTIONS_RGN"); 
      convertToJson ("SETTLEMENT_IDS_RGN"); 
      convertToJson ("RISK_LIMIT_GROUPS_RGN");
      convertToJson ("RISK_LIMIT_ASSOCIATIONS_RGN");
      convertToJson ("TRADING_RESTRICTIONS_RGN");
      convertToJson ("ACCOUNTS_RGN");
      convertToJson ("PORTFOLIOS_RGN");
      convertToJson ("TRADING_BOOKS_RGN");
      convertToJson ("ORDER_EXEC_INFO_RGN");
      convertToJson ("ORDERS_RGN");
    }


    createDataTable ("CACHE_STATUS", m_initCache, m_cacheSize); 
    createDataTable ("MARKET_QUOTES_RGN", m_initCache, m_cacheSize); 
    createDataTable ("INTERNAL_QUOTES_RGN", m_initCache, m_cacheSize); 
    createDataTable ("QUOTE_SUBSCRIPTIONS_RGN", m_initCache, m_cacheSize); 
    createDataTable ("INTERNAL_QUOTE_SUBSCRIPTIONS_RGN", m_initCache, m_cacheSize); 
    createDataTable ("SETTLEMENT_IDS_RGN", m_initCache, m_cacheSize); 
    createDataTable ("ORDER_LOCK_STATUS", m_initCache, m_cacheSize); 
    createDataTable ("RISK_LIMIT_GROUPS_RGN", m_initCache, m_cacheSize);
    createDataTable ("RISK_LIMIT_ASSOCIATIONS_RGN", m_initCache, m_cacheSize);
    createDataTable ("TRADING_RESTRICTIONS_RGN", m_initCache, m_cacheSize);
    createDataTable ("ACCOUNTS_RGN", m_initCache, m_cacheSize);
    createDataTable ("PORTFOLIOS_RGN", m_initCache, m_cacheSize);
    createDataTable ("TRADING_BOOKS_RGN", m_initCache, m_cacheSize);
    createDataTable ("ORDER_EXEC_INFO_RGN", m_initCache, m_cacheSize);
    createDataTable ("ORDERS_RGN", m_initCache, m_cacheSize);



    return;
  }

  // ----------------------------------------
  // createDataTable ()
  // Creates a data copy of the given table
  private void createDataTable (String tableName, boolean recreate, 
    int maxRows)
    throws SQLException
  {
    String dataTableName = tableName + "_DATA";
    boolean tableExists = false;

    // truncate SQL identifier to 30 chars
    if (dataTableName.length () > 30)
      dataTableName = dataTableName.substring (0, 30);

    String dropSql = "DROP TABLE " + dataTableName;

    String createSql = "CREATE TABLE " + dataTableName + " AS " + 
          "SELECT FIRST " + maxRows + " * FROM " + tableName;

    Connection conn = checkoutConn ("TradeManager");

    DatabaseMetaData dbmd = conn.getMetaData ();
    ResultSet rs = dbmd.getTables (null, null, dataTableName, null);

    if (rs.next ())
    {
      tableExists = true;
    }

    rs.close ();


    // drop the data table
    if (recreate && tableExists) 
    {
      log ("TradeManager", "Dropping data table " + dataTableName + " ...");

      Statement stmt = conn.createStatement ();
      stmt.executeUpdate (dropSql);
      stmt.close ();

      tableExists = false;
    }
    

    // create the data table
    if (!tableExists)
    {
      log ("TradeManager", "Creating data table " + dataTableName + 
        " with up to " + maxRows + " rows ...");

      Statement stmt = conn.createStatement ();
      stmt.executeUpdate (createSql);
      stmt.close ();
    } 
    else
    {
      log ("TradeManager", "Data table " + dataTableName + " exists");      
    }


    checkinConn ("TradeManager", conn);

    return;
  }

// ----------------------------------------
  // convertToJson ()
  // Alter the do_json column to the JSON data type
  private void convertToJson (String tableName)
    throws SQLException
  {
    boolean tableColFound = false;
    String typeName;
    String sql;
    Connection conn = null;
    Statement stmt = null;

    log ("TradeManager", "Altering table " + tableName + " for the JSON type ...");
    

    try
    {
      conn = checkoutConn ("TradeManager");
      stmt = conn.createStatement ();

      /* what columns exist? */
      DatabaseMetaData dbmd = conn.getMetaData ();
      ResultSet rs = dbmd.getColumns (null, null, tableName, "DO_JSON");

      if (rs.next ())
        tableColFound = true;

      rs.close ();

      if (!tableColFound)
      {
        checkinConn ("TradeManager", conn);
        log ("TradeManager", "The DO_JSON column was not found");
        return;
      }

      // drop the JSON_TEMP column - it may exist from a previous attempt
      try
      {
        sql = "alter table " + tableName + " drop column json_temp";
        stmt.executeUpdate(sql);
      }
      catch (SQLException ex)
      { /* do nothing */}


      /* convert the DO_JSON column to the JSON data type */
      sql = "alter table " + tableName + " add column json_temp json";
      stmt.executeUpdate(sql);

      sql = "update " + tableName + " set json_temp = do_json";
      stmt.executeUpdate(sql);

      sql = "alter table " + tableName + " drop column do_json";
      stmt.executeUpdate(sql);

      sql = "alter table " + tableName + " add column do_json json";
      stmt.executeUpdate(sql);

      sql = "update " + tableName + " set do_json = json_temp";
      stmt.executeUpdate(sql);

      sql = "alter table " + tableName + " drop column json_temp";
      stmt.executeUpdate(sql);
    }
    catch (SQLException ex)
    {
      throw ex;
    }
    finally
    {
      if (conn != null)
        checkinConn ("TradeManager", conn);
      
      if (stmt != null)
        stmt.close ();
    }

  }


  // ----------------------------------------
  // cacheDataTables ()
  // Caches copies of data tables
  private void cacheDataTables () 
    throws SQLException
  {

    cacheTable ("CACHE_STATUS_DATA");
    cacheTable ("MARKET_QUOTES_RGN_DATA");
    cacheTable ("INTERNAL_QUOTES_RGN_DATA");
    cacheTable ("QUOTE_SUBSCRIPTIONS_RGN_DATA");
    cacheTable ("INTERNAL_QUOTE_SUBSCRIPTIONS_R");
    cacheTable ("SETTLEMENT_IDS_RGN_DATA");
    cacheTable ("ORDER_LOCK_STATUS_DATA");
    cacheTable ("RISK_LIMIT_GROUPS_RGN_DATA");
    cacheTable ("RISK_LIMIT_ASSOCIATIONS_RGN_DA");
    cacheTable ("TRADING_RESTRICTIONS_RGN_DATA");
    cacheTable ("ACCOUNTS_RGN_DATA");
    cacheTable ("PORTFOLIOS_RGN_DATA");
    cacheTable ("TRADING_BOOKS_RGN_DATA");
    cacheTable ("ORDER_EXEC_INFO_RGN_DATA");
    cacheTable ("ORDERS_RGN_DATA");


    return;
  }

  // ----------------------------------------
  // cacheTable ()
  // Caches copies of tables
  private boolean cacheTable (String tableName) 
    throws SQLException
  {
    boolean status = true;
    Statement stmt = null;
    ResultSet rs = null;
    Connection conn = null;
    ResultSetMetaData rsmd = null;
    int index, columnCount;

    ArrayList<HashMap<String, Object>> resultList = new ArrayList<>();

    log ("TradeManager", "Caching table " + tableName + " ...");

    conn = checkoutConn("TradeManager");


    try
    {
        stmt = conn.createStatement ();
        rs = stmt.executeQuery ("SELECT * FROM " + tableName);

        rsmd = rs.getMetaData();
        columnCount = rsmd.getColumnCount();

        while (rs.next ())
        {
          HashMap<String, Object> row = new HashMap<>();

          for (index = 1; index <= columnCount; index ++)
          {
            if (rsmd.getColumnType(index) == java.sql.Types.CLOB)
            {
              Clob clob = rs.getClob (index);

              row.put (rs.getMetaData ().getColumnName (index), 
                clob.getSubString (1, (int) clob.length()));

              clob.free ();
            }
            else
            {
              row.put (rs.getMetaData ().getColumnName (index), 
                rs.getObject (index));
            }
          }

          resultList.add(row);
        }
    }
    finally
    {
        if (rs != null)
          rs.close ();

        if (stmt != null)
          stmt.close ();
    }

 
    checkinConn ("TradeManager", conn);

    log ("TradeManager", "Cached " + resultList.size () + " rows");
    m_tableCache.put (tableName, resultList);

    return status;
  }


  // ----------------------------------------
  // getCachedValue ()
  // Returns a data value from the given table and column. The value is selected
  // randomly among the rows in the table.
  public Object getCachedValue (String source, String tableName, String columnName) 
  {
    int targetRowIndex;
    HashMap<String, Object> row;
    Object value = null;

    ArrayList<HashMap<String, Object>> resultList = m_tableCache.get (tableName);

    if (resultList == null || resultList.size () == 0)
    {
       log (source, "Table " + tableName + " has no rows");
       return value;   
    }


    log (source, "Getting random value from " + tableName + 
      "." + columnName + " ...");

    targetRowIndex = m_rand.nextInt (resultList.size ());
    row = resultList.get (targetRowIndex);

    value = row.get (columnName);
    log (source, columnName + " = " + value);

    return value;
  }

  // ----------------------------------------
  // getCachedRow ()
  // Returns a row as a HashMap from the given table. The row is selected
  // randomly among the rows in the table.
  public HashMap<String, Object> getCachedRow (String source, String tableName) 
  {
    int targetRowIndex;
    HashMap<String, Object> row = null;


    ArrayList<HashMap<String, Object>> resultList = m_tableCache.get (tableName);

    if (resultList == null || resultList.size () == 0)
    {
      return row;
    }

    log (source, "Getting random row from " + tableName + " ...");

    log (source, "Result set size: " + resultList.size ());
    targetRowIndex = m_rand.nextInt (resultList.size ());

    row = resultList.get (targetRowIndex);

    log (source, "Row = " + row);

    return row;
  }





  // ----------------------------------------
  // buildConnectionPool ()
  // Fills the connection pool with connections
  private void buildConnectionPool ()
    throws SQLException
  {
    // locals
    int index = 0;    
    Connection conn = null;

    log ("TradeManager", "Building connection pool ...");

    m_dataSource = new TimesTenDataSource ();
    m_conn = new Connection [m_poolSize];

    // set the url...
    ((TimesTenDataSource)m_dataSource).setUrl (m_url);
    
    for (index = 0; index < m_poolSize; index ++)
    {
      // create the connection
      m_conn [index] = m_dataSource.getConnection ();
      log ("TradeManager", "Opened connection #" + index + " successfully.");
    }

    return; 
  }


  // ----------------------------------------
  // checkoutConn ()
  // Checks out a connection from the pool - this method is used by
  // the TradeThread class to get a SQL connection to work with
  public synchronized Connection checkoutConn (String source)
  {
    // locals
    int index = 0;
    Connection conn = null;
    
    
    for (index = 0; index < m_poolSize; index ++)
    {
      conn = m_conn [index];
      
      if (conn != null)
      {
        // set the array value to null to indicate that the connection
        // is now checked out
        m_conn [index] = null;
        break;
      }
    }
    
    
    return conn;
  }
  
  
  // ----------------------------------------
  // checkinConn ()
  // Checks a connection back into the connection pool
  public synchronized void checkinConn (String source, Connection conn)
  {
    // locals 
    int index = 0;

    for (index = 0; index < m_poolSize; index ++)
    {
      if (m_conn [index] == null)
      {
        m_conn [index] = conn;
        break;
      }
    }
    
    return;
  }
  
  
  
  
  
  // ----------------------------------------
  // log ()
  // All info messages are directed through here
  public void log (String source, String message)
  {
    
    if (!m_quiet)
    {
      synchronized (this)
      {
        System.out.println (source + " : " + message);
      }

      System.out.flush ();
    }

    return;
  }

  // ----------------------------------------
  // logForce ()
  // Write an info message even when the test is in quiet mode
  public synchronized void logForce (String source, String message)
  {

    System.out.println (source + " : " + message);
    System.out.flush ();

    return;
  }
  
  // ----------------------------------------
  // logErr ()
  // All error messages are directed through here
  public synchronized void logErr (String source, String message)
  {
    
    System.err.println ("");
    System.err.println (source + " : " + message);
    System.err.println ("");
    
    System.err.flush ();
    
    return;
  }
  
  
  
  
  // -----------------------------------------------------
  // handleSQLException ()
  // Reports information on an SQLException object
  public synchronized void handleSQLException (String source, 
    java.sql.SQLException ex)
  {
    
    
    while (ex != null)
    {
      logErr (source, "SQLException: " + ex.getMessage ());
      
      // print the stack trace
      ex.printStackTrace ();
      
      // if the SQLException message is invalid then throw a fatal
      // error
      if (ex.getMessage () == null)
        throw new java.lang.Error ("Invalid SQLException object.");
      
      // get the next exception if one exists
      ex = ex.getNextException ();
    }
    
    
    
    return;
  }

  // -----------------------------------------------------
  // handleException ()
  // Reports information on a generic Exception object
  public synchronized void handleException (String source, Exception ex)
  {
    
    logErr (source, "Exception: " + ex.getMessage ());
    
    // print the stack trace
    ex.printStackTrace ();
 
    return;
  }
  
  // -----------------------------------------------------
  // areThreadsAlive ()
  // Returns true if any TradeThread thread in the m_threads
  // Vector is running
  private boolean areThreadsAlive ()
  {
    // locals
    boolean status = false;
    int index = 0;
    TradeThread [] threads = new TradeThread [1];
    
    // get an array of TradeThreads
    threads = (TradeThread[]) m_threads.toArray (threads);
    
    // check every thread - if any are alive return true
    for (index = 0; index < threads.length; index ++)
    {
      if (threads [index].isAlive ())
      {
        status = true;
        break;
      }
    }
    
    
    return status;
  }
  
  
  // -----------------------------------------------------
  // didThreadsSucceed ()
  // Returns true if any TradeThread in the m_threads Vector has a FAIL status
  private boolean didThreadsSucceed ()
  {
    boolean status = true;
    int index = 0;
    TradeThread [] threads = new TradeThread [1];
    
    if (!m_threads.isEmpty ())
    {
      // get an array of threads
      threads = (TradeThread[]) m_threads.toArray (threads);
      
      // check every thread - if any are FAILED return false
      for (index = 0; index < threads.length; index ++)
      {
        if (!threads [index].isAlive () && !threads [index].getStatus ())
        {
          logErr ("TradeManager", "FAILED");  
          status = false;
        }
        else if (!threads [index].isAlive ())
        {
          // if the thread passed and it is not active then remove the
          // thread from the m_threads Vector in order to garbage
          // collect the TradeThread object
          m_threads.remove (threads [index]);
        }
      }
    }
    
    
    return status;
  }
  

  // ----------------------------------------
  // setTiming ()
  // Adds a timing observation for a procedure
  public synchronized void setTiming (String procedure, long elapsedTime)
  {
    m_timingProcs.add (procedure);
    m_timings.add (elapsedTime);
  }

  // ----------------------------------------
  // calcAvgElapsedTime () 
  // Returns the average time in us
  private double calcAvgElapsedTime(Vector<Long> timings) 
  {

    if (timings.isEmpty()) 
      return 0; // Return 0 for empty list to avoid division by zero


    Iterator iter = timings.iterator();
    long sum = 0;

    while (iter.hasNext()) 
    {
      sum += (long) iter.next ();
    }

    return (double) sum / timings.size();
  }

  
  // ----------------------------------------
  // reportSummaryStats () 
  // Reports various summary and performance stats
  private void reportSummaryStats () 
  {
    String displayValue;


    logForce ("TradeManager", "URL: " + m_url);
    
    logForce ("TradeManager", "Total transactions: " + 
      m_timings.size());

    logForce ("TradeManager", "Thread count: " + m_threadCount);

    // calculate TPS
    double elapsedSecs = (m_timer.getTimeInUs () / 1000d) / 1000d;
    displayValue = String.format ("%.2f", elapsedSecs);
    logForce ("TradeManager", "Duration (seconds): " + displayValue);

    long totalTransactions = m_timings.size ();
    double tps = totalTransactions / elapsedSecs;
    displayValue = String.format ("%.2f", tps);    
    logForce ("TradeManager", "TPS: " + displayValue);

    displayValue = String.format ("%.2f", calcAvgElapsedTime (m_timings));    
    logForce ("TradeManager", "All transactions avg. time (us): " + 
      displayValue);


    // get a distinct list of procedure names
    HashMap<String, Double> summaryStats = new HashMap<> ();
    Set<String> timingProcsDistinct = new HashSet<>(m_timingProcs);

    // calculate the average by each procedure
    for (String proc : timingProcsDistinct) 
    {
      Vector<Long> timings = new Vector<>();
      
      for (int index = 0; index < m_timings.size(); index ++)
      {
        if (m_timingProcs.elementAt(index).equals(proc))
          timings.add (m_timings.elementAt(index));
      }

      summaryStats.put (proc, calcAvgElapsedTime (timings));
    }


    // sort the list by execution time
    ArrayList<Double> list = new ArrayList<>();
    LinkedHashMap<String, Double> sortedMap = new LinkedHashMap<>();

    for (Map.Entry<String, Double> entry : summaryStats.entrySet ()) 
      list.add (entry.getValue());

    Collections.sort (list); 

    for (double value : list) 
    {
      for (Map.Entry<String, Double> entry : summaryStats.entrySet()) 
      {
          if (entry.getValue().equals(value)) 
            sortedMap.put(entry.getKey(), value);
      }
    }


    // get the count of each distinct transactions
    HashMap<String, Integer> execCountsMap = new HashMap<>();

    for (String element : m_timingProcs) 
    {
        execCountsMap.put(element, execCountsMap.getOrDefault(element, 0) + 1);
    }

    /*
    for (Map.Entry<String, Integer> entry : execCountsMap.entrySet()) {
        System.out.println(entry.getKey() + ": " + entry.getValue());
    }
    */


    // print the results
    for (Map.Entry<String, Double> entry : sortedMap.entrySet()) 
    {
      displayValue = String.format ("%.2f", entry.getValue());
      int txnCount = execCountsMap.get (entry.getKey());

      logForce ("TradeManager", 
        entry.getKey() + " avg. time (us): " + displayValue + 
          " (" + txnCount + " txns.)");
    }


    // write the TPS csv file - append if it already exists
    try
    {
      FileWriter fw = new FileWriter (TRADE_TPS_FILE, true);
      BufferedWriter bw = new BufferedWriter (fw);

      // <tps>, <thread count>, <txn count per thread>, <url>
      bw.write (Double.toString (tps));
      bw.write (", " + Integer.toString (m_threadCount));
      bw.write (", " + Integer.toString (m_txns));
      bw.write (", " + m_url);

      bw.newLine();
      bw.close();  
      
      logForce ("TradeManager", "Wrote TPS record: " + TRADE_TPS_FILE);
    }
    catch (IOException ex)
    {
      handleException("TradeManager", ex);
    }


    // write the response time csv file - append if it already exists
    try
    {
      FileWriter fw = new FileWriter (TRADE_RESPONSE_FILE, true);
      BufferedWriter bw = new BufferedWriter (fw);

      // <procedure>, <avg response (us)>, <thread count>, <txn count per thread>, <url>  
      for (Map.Entry<String, Double> entry : sortedMap.entrySet()) 
      {
        bw.write (entry.getKey ());
        bw.write (", " + entry.getValue());
        bw.write (", " + Integer.toString (m_threadCount));
        bw.write (", " + Integer.toString (m_txns));
        bw.write (", " + m_url); 

        bw.newLine();
      }

      bw.close();  
      
      logForce ("TradeManager", "Wrote response time records: " + TRADE_RESPONSE_FILE);
    }
    catch (IOException ex)
    {
      handleException("TradeManager", ex);
    }


    return;
  }


  /* Return a hash map that includes the name of all available transactions */
  /* and their ids. */
  public static LinkedHashMap<String, Integer> getDefaultTxnMap ()
  {

    LinkedHashMap<String, Integer> txnMap = new LinkedHashMap <>();

    txnMap.put ("del_accounts_rgn", 1);
    txnMap.put ("del_market_quotes_rgn", 2);
    txnMap.put ("del_order_exec_info_rgn", 3);
    txnMap.put ("del_portfolios_rgn", 4);
    txnMap.put ("del_internal_quote_subscriptions_rgn", 5);
    txnMap.put ("del_internal_quotes_rgn", 6);
    txnMap.put ("del_settlement_ids_rgn", 7);
    txnMap.put ("del_quote_subscriptions_rgn", 8);
    txnMap.put ("del_trading_books_rgn", 9);
    txnMap.put ("del_risk_limit_groups_rgn", 10);
    txnMap.put ("del_trading_restrictions_rgn", 11);
    txnMap.put ("del_risk_limit_associations_rgn", 12);
    txnMap.put ("put_risk_limit_associations_rgn", 13);
    txnMap.put ("put_settlement_ids_rgn", 14);
    txnMap.put ("put_order_exec_info_rgn", 15);
    txnMap.put ("del_orders_rgn", 16);
    txnMap.put ("put_internal_quote_subscriptions_rgn", 17);
    txnMap.put ("put_portfolios_rgn", 18);
    txnMap.put ("put_quote_subscriptions_rgn", 19);
    txnMap.put ("put_market_quotes_rgn", 20);
    txnMap.put ("put_trading_restrictions_rgn", 21);
    txnMap.put ("put_trading_books_rgn", 22);
    txnMap.put ("put_risk_limit_groups_rgn", 23);
    txnMap.put ("put_accounts_rgn", 24);
    txnMap.put ("put_internal_quotes_rgn", 25);
    txnMap.put ("put_orders_rgn", 26);
    txnMap.put ("put_cache_status", 27);
    txnMap.put ("query_get_market_quote", 28);
    txnMap.put ("query_get_internal_quote", 29);
    txnMap.put ("query_get_risk_limit_groups", 30);
    txnMap.put ("query_get_order", 31);
    txnMap.put ("query_get_settlement_id", 32);

    return txnMap;
  }


  /* Returns a hash map of transactions and ids based on a comma */
  /* delimited list of transaction codes. If txnCodes is null then */
  /* the default set of all transactions is returned. */
  public LinkedHashMap<String, Integer> getTxnMap (String txnCodes)
    throws Exception
  {
    LinkedHashMap<String, Integer> defaultTxnMap = getDefaultTxnMap ();
    LinkedHashMap<String, Integer> txnMap = new LinkedHashMap <>();

    // return the default map if txnCodes is null
    if (txnCodes == null)
      return defaultTxnMap;

    String[] codes = txnCodes.split (",");

    for (String code : codes)
    {
      for (Map.Entry<String, Integer> entry : defaultTxnMap.entrySet()) 
      {
        String key = entry.getKey ();
        int value = entry.getValue ();

        if (value == Integer.parseInt (code))
        {
          txnMap.put (key, value);
        }
      }
    }

    // the map must have at least one element
    if (txnMap.size() == 0)
    {
      throw new Exception ("Empty transaction list, check transaction codes");     
    }


    return txnMap;
  }

  /* Return a hash map that includes the name of all available transactions */
  /* and their execution probabalities. */
  public static LinkedHashMap<String, Integer> getDefaultTxnProbMap ()
  {

    LinkedHashMap<String, Integer> probMap = new LinkedHashMap <>();

    probMap.put ("del_accounts_rgn", 3);
    probMap.put ("del_market_quotes_rgn", 3);
    probMap.put ("del_order_exec_info_rgn", 3);
    probMap.put ("del_portfolios_rgn", 3);
    probMap.put ("del_internal_quote_subscriptions_rgn", 3);
    probMap.put ("del_internal_quotes_rgn", 3);
    probMap.put ("del_settlement_ids_rgn", 3);
    probMap.put ("del_quote_subscriptions_rgn", 3);
    probMap.put ("del_trading_books_rgn", 3);
    probMap.put ("del_risk_limit_groups_rgn", 3);
    probMap.put ("del_trading_restrictions_rgn", 3);
    probMap.put ("del_risk_limit_associations_rgn", 3);
    probMap.put ("put_risk_limit_associations_rgn", 3);
    probMap.put ("put_settlement_ids_rgn", 3);
    probMap.put ("put_order_exec_info_rgn", 3);
    probMap.put ("del_orders_rgn", 3);
    probMap.put ("put_internal_quote_subscriptions_rgn", 3);
    probMap.put ("put_portfolios_rgn", 3);
    probMap.put ("put_quote_subscriptions_rgn", 3);
    probMap.put ("put_market_quotes_rgn", 3);
    probMap.put ("put_trading_restrictions_rgn", 3);
    probMap.put ("put_trading_books_rgn", 3);
    probMap.put ("put_risk_limit_groups_rgn", 3);
    probMap.put ("put_accounts_rgn", 3);
    probMap.put ("put_internal_quotes_rgn", 3);
    probMap.put ("put_orders_rgn", 3);
    probMap.put ("put_cache_status", 3);
    probMap.put ("query_get_market_quote", 3);
    probMap.put ("query_get_internal_quote", 4);
    probMap.put ("query_get_risk_limit_groups", 4);
    probMap.put ("query_get_order", 4);
    probMap.put ("query_get_settlement_id", 4);

    return probMap;
  }

  // Given a set of transactions, build a probability map that is uniform - that
  // selects transactions with equal probabilities.
  public static LinkedHashMap<String, Integer> buildUniformTxnProbMap (
    LinkedHashMap<String, Integer> txnMap)
  {
    LinkedHashMap<String, Integer> txnProbsMap = new LinkedHashMap <>();

    // what is the probability for each transaction
    double value = 100 / txnMap.size();

    for (Map.Entry<String, Integer> entry : txnMap.entrySet()) 
    {
      String key = entry.getKey ();
      txnProbsMap.put (key, (int) value);
    }   
    
    // the transaction probabilities must sum to 100
    int probSum = 0;

    for (Integer prob : txnProbsMap.values())
    {
      probSum += prob; 
    }  

    // if the probabilities do not sum to 100 then increase them until they do 
    if (probSum < 100)
    {
      int probDelta = 100 - probSum;

      while (true) 
      {
        for (Map.Entry<String, Integer> entry : txnProbsMap.entrySet()) 
        {
          txnProbsMap.put (entry.getKey (), entry.getValue () + 1);
          probDelta = probDelta - 1;

          if (probDelta == 0)
            break;
        } 

        if (probDelta == 0)
          break;        
      }
    }

    return txnProbsMap;
  }




  /* Returns a hash map of transaction probabilities based on a comma */
  /* delimited list of probabilites summing to 100. If txnProbs is null then */
  /* the default set of probabilities is returned. */
  public LinkedHashMap<String, Integer> getTxnProbsMap (
    LinkedHashMap<String, Integer> txnMap, String txnProbs)
    throws Exception
  {
    LinkedHashMap<String, Integer> txnProbsMap = new LinkedHashMap <>();

    // if txnProbs is null then return a uniform distribution map
    if (txnProbs == null)
      return buildUniformTxnProbMap (txnMap);

    String[] probs = txnProbs.split (",");


    // the number of probabilities must equal the number of transactions
    if (probs.length != txnMap.size())
    {
      throw new Exception (
        "The number of transaction probabilites (" + probs.length + 
        ") does not match the number of transactions (" + txnMap.size() + ")"); 
    }

    // the probabilities must sum to 100
    int probSum = 0;

    for (String prob: probs)
    {
      probSum += Integer.parseInt (prob);
    }

    if (probSum != 100)
    {
      throw new Exception ("The transaction probabilites sum to " + probSum + 
        " instead of 100.");
    }

    int mapCount = txnProbsMap.size();
    int index = 0;

    for (String prob: probs)
    {
      // get the key from txnMap
      Object [] txnArrayKeys = txnMap.keySet().toArray ();
        
      txnProbsMap.put (txnArrayKeys [index].toString(), 
        Integer.parseInt (prob));

      index ++;
    }

    // the map must have at least one element
    if (txnProbsMap.isEmpty ())
    {
      throw new Exception ("Empty probability list");     
    }


    return txnProbsMap;
  }  






}
