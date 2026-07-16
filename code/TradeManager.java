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


}
