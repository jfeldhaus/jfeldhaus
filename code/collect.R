# collect.R


# These procedures incrementally collect data for test runs defined in the STF
# TEST_RUN table.



# Top level procedure to run collections for all active test runs. If the
# collection procedure is successful then TRUE is returned.
collect <- function () {

  props <- getSTFProps ()

  loginfo ("[%s] Running collection procedures for all active test runs",
           "collect")

  # default return code
  rc <- TRUE



  tryCatch ({

    # acquire a connection to the STF database and get a list of active
    # test runs
    stf.conn <- getSTFConn ()
    test.runs <- getActiveTestRuns (stf.conn)

    
    # order by id - this is necessary with classic collection so
    # that the sample interval is approximately the same for every
    # collection
    test.runs <- arrange(test.runs, TEST_RUN_ID)
    
    
    
    loginfo ("[%s] Found %s active test run(s)",
             "collect", nrow (test.runs))

    if (nrow (test.runs) > 0) {

      for (i in 1:nrow (test.runs)) {

        # parallel collection?
        if (as.integer (props$stf.collect.parallel)) {

          # execute the collection procedure in parallel for this test run
          if (!collectTestRunParallel (stf.conn, test.runs [i,]))
            rc <- FALSE

        } else {

          # execute the collection procedure sequentially for this test run
          if (!collectTestRun (stf.conn, test.runs [i,]))
            rc <- FALSE

        }

      }
    }

  },

  error = function (e) {

    rc <<- FALSE
    logerror ("[%s] %s ", "collect", e [[1]])
  },

  finally = {

    # clean up
    try (dbRollback (stf.conn), silent = TRUE)
    try (dbDisconnect (stf.conn), silent = TRUE)
  })



  loginfo ("[%s] Done", "collect")

  return (rc)
}


# Perform collection tasks for a given (active) test run using parallel
# collection processes. This is not currently supported on Windows. If all 
# collection procedure succeed then TRUE is returned.
collectTestRunParallel <- function (stf.conn, test.run) {


  stf.props <- getSTFProps ()
  child.timeout <- as.integer (stf.props$stf.collect.parallel.timeout.secs)

  # default return code
  rc <- TRUE


  loginfo ("[%s] Running parallel collection procedures for TEST_RUN_ID = %s",
           "collectTestRunParallel", test.run$TEST_RUN_ID)



  # Note that all collection procedures must establish their own database
  # connections because mcparallel() uses fork().
  tryCatch ({

   
    # run the file system based collection procedures
    collect.logs.job <- mcparallel ({

      tryCatch ({

        # This avoids a logging module bug/exception when a forked process 
        # attempts to write to the xterm console.
        configureLogging (getSTFProps (), console = FALSE)

        # a new connection is required when forking
        conn <- getSTFConn ()

        collectLogs (conn, test.run)
      },

      error = function (e) {
        logerror ("[%s] %s ", "collectTestRunParallel", e [[1]])
        stop (e [[1]])
      },

      finally = {
        try (dbRollback (conn), silent = TRUE)
        try (dbDisconnect (conn), silent = TRUE)
      })

    },
    
    silent = FALSE,
    mc.interactive = TRUE,
    detached = FALSE)


    
    # run the SQL based collection procedures
    collect.sql.job <- mcparallel ({
      
      tryCatch ({
        
        # This avoids a logging module bug/exception when a forked process 
        # attempts to write to the xterm console.
        configureLogging (getSTFProps (), console = FALSE)
        
        # a new connection is required when forking        
        conn <- getSTFConn ()
        
        collectTestRun (conn, test.run,
                        collect.logs = FALSE,
                        collect.events = FALSE,
                        update.refreshed = FALSE)
      },
      
      error = function (e) {
        logerror ("[%s] %s ", "collectTestRunParallel", e [[1]])
        stop (e [[1]])
      },
      
      finally = {
        try (dbRollback (conn), silent = TRUE)
        try (dbDisconnect (conn), silent = TRUE)
      })
      
    },
    
    silent = FALSE,
    mc.interactive = TRUE,
    detached = FALSE)
    
    
    
    

    # wait for the SQL collection job to complete
    loginfo (
      "[%s] Waiting for collectTestRun (), PID = %s, timeout = %s seconds ...",
      "collectTestRunParallel",
      collect.sql.job$pid,
      child.timeout)

    collect.sql.job.result <- mccollect (
      collect.sql.job,

      wait = ifelse (child.timeout == 0, TRUE, FALSE),
      timeout = child.timeout)
    
    
    # process results
    if (!mccollectResults (collect.sql.job.result,
                           throw.exception = FALSE)) {
      
      logerror (
        "[%s] An exception occurred during parallel SQL collection",
        "collectTestRunParallel")
      
      rc <- FALSE      
    }
    
    # timeout then kill
    if (is.null (collect.sql.job.result)) {

      logerror (
        "[%s] The collectTestRun() child process PID %s timed out",
        "collectTestRunParallel",
        collect.sql.job$pid)

      killProcess (collect.sql.job$pid)

      rc <- FALSE
    }
    
    
    
    
    # wait for the instance log collection job to complete
    loginfo ("[%s] Waiting for collectLogs (), PID = %s ...",
             "collectTestRunParallel",
             collect.logs.job$pid)
    

    collect.logs.job.result <- mccollect (
      collect.logs.job,

      wait = ifelse (child.timeout == 0, TRUE, FALSE),
      timeout = child.timeout)

    
    # process results
    if (!mccollectResults (collect.logs.job.result,
                           throw.exception = FALSE)) {
      
      logerror (
        "[%s] An exception occurred during parallel instance log collection",
        "collectTestRunParallel")
      
      rc <- FALSE      
    }
    
    
    # timeout then kill
    if (is.null (collect.logs.job.result)) {

      logerror (
        "[%s] The collectLogs() child process PID %s timed out",
        "collectTestRunParallel",
         collect.logs.job$pid)

      killProcess (collect.logs.job$pid)

      rc <- FALSE
    }


    
    # detect new events after collection has completed
    collectTestRunEvents (stf.conn, test.run$TEST_RUN_ID)

  },

  error = function (e) {
    rc <<- FALSE

    logerror ("[%s] Parallel collection procedure failed: %s",
              "collectTestRunParallel", e [[1]])

    try (dbRollback (stf.conn))
  },

  finally = {

    # update the time of the latest collection
    setTestRunRefreshed (stf.conn, test.run$TEST_RUN_ID)
  })

  
  
  if (rc) {
    
    loginfo ("[%s] Collection procedures complete",
             "collectTestRunParallel")
  } else {
    
    logerror ("[%s] Some collection procedure(s) failed",
              "collectTestRunParallel")    
  }
  
  return (rc)
}


# Update the INSTANCE_HISTORY table with the latest instance states
collectInstances <- function (stf.conn, test.run, tt.conn) {


  loginfo ("[%s] Collecting instance info for TEST_RUN_ID = %s",
           "collectInstances", test.run$TEST_RUN_ID)


  # refresh the deployment for this test run
  rc <- refreshDeployment (test.run$DEPLOYMENT_ID)


  if (!rc) {

    # this procedure throws an exception on error
    stop ("refreshDeployment() failed")

  } else {

    # If the deployment refresh was successful then the DB_INSTANCE table
    # will contain the latest info. Merge this info into the test run's
    # INSTANCE_HIST table.


    # retrieve the latest instance info from the DB_INSTANCE table
    db.instances <- getInstances (stf.conn, test.run$DEPLOYMENT_ID)

    # drop columns that should not be compared
    db.instances <- db.instances [,
      !(names (db.instances) %in% c ("DEPLOYMENT_ID"))]


    if (nrow (db.instances) > 0) {

      # Is anything different about the current data set versus the previous
      # data set stored in the INSTANCE_HIST table for this test run? If there
      # isn't any difference then don't insert.
      db.instances.prev <- getTestRunInstances (stf.conn, test.run$TEST_RUN_ID)

      # drop columns that should not be compared
      db.instances.prev <- db.instances.prev [,
        !(names (db.instances.prev) %in% c ("TEST_RUN_ID", "COLLECTED_AT"))]

      prev.row.count <- nrow (db.instances.prev)
      diff.row.count <- nrow (dplyr::union (db.instances, db.instances.prev))

      if (prev.row.count == diff.row.count) {

        loginfo ("[%s] No instance state changes detected",
                 "collectInstances")

      } else {

        # prepare the data set for insert - the COLLECTED_AT timestamp must be
        # the same for every instance record (of this particular sample) in
        # order to support the history function of this table
        db.instances$TEST_RUN_ID <- test.run$TEST_RUN_ID
        db.instances$COLLECTED_AT <- Sys.time ()

        db.instances <- select (db.instances,
                                TEST_RUN_ID,
                                COLLECTED_AT,
                                DB_HOST,
                                DB_PORT,
                                DB_NAME,
                                DB_STATE,
                                INSTANCE_DIR,
                                INSTANCE_NAME,
                                ELEMENTID,
                                REPSETID,
                                DATASPACEID,
                                DATASTORE)

        # write the new info
        dbWriteTable (stf.conn, "INSTANCE_HIST", db.instances, append = TRUE)
        dbCommit (stf.conn)


        # update the ODBC configuration files so that the alternate server list
        # is updated based on the instance changes
        createODBCConfig ()

        loginfo ("[%s] New instance info records committed",
                 "collectInstances")
      }
    }

  }

}


# Update the TTSTATS_CPU_HIST table with CPU metrics.
collectCpuMetrics <- function (stf.conn, test.run, tt.conn) {


  loginfo ("[%s] Collecting OS metrics for TEST_RUN_ID = %s",
           "collectCpuMetrics", test.run$TEST_RUN_ID)


  # get the last time CPU metrics were collected for this test run
  last.collect.date <- getLastCollectDate (stf.conn,
                                           test.run$TEST_RUN_ID,
                                           "TTSTATS_CPU_HIST")

  loginfo ("[%s] Last CPU history collection date = %s",
           "collectCpuMetrics", last.collect.date$COLLECTED_AT)


  query <-
    "
    SELECT 

      ELEMENTID,
      ID AS COLLECT_ID,
      COLLECTED_AT,
      CPU_UTIL

    FROM GV$TTSTATS_CPU_HIST CPU
    "

  # when a previous collection exists constrain the current query based on the
  # last collection time
  if (!is.na (last.collect.date$COLLECTED_AT)) {

    # DATE format: YYYY-MM-DD HH:MI:SS
    query <- paste0 (query, "\n WHERE COLLECTED_AT > ",
                     getPOSIXctAsSQL (last.collect.date$COLLECTED_AT))
  }




  # query GV$TTSTATS_CPU_HIST for specific metrics
  os.metrics <- dbGetQuery (tt.conn, query)


  loginfo ("[%s] Collected %s new CPU records",
           "collectCpuMetrics", nrow (os.metrics))




  if (nrow (os.metrics) > 0) {

    # insert the new records into TTSTATS_CPU_HIST
    os.metrics$TEST_RUN_ID <- test.run$TEST_RUN_ID
    os.metrics <- select (os.metrics,
                          TEST_RUN_ID,
                          ELEMENTID,
                          COLLECT_ID,
                          COLLECTED_AT,
                          CPU_UTIL)


    dbWriteTable (stf.conn, "TTSTATS_CPU_HIST", os.metrics, append = TRUE)
    dbCommit (stf.conn)

    loginfo ("[%s] New CPU records committed", "collectCpuMetrics")
  }
}


# This is a utility function that process results from the mccollect function
# used to retrieve results from asynchronously executed code via the mcparallel
# function. The function takes the return value from mcccollect (results) and 
# logs all return values. If a job return value indicates an exception then this
# function will return FALSE, otherwise TRUE. If throw.exception is TRUE then
# the first exception detected in the results will be thrown.
mccollectResults <- function (results, 
                              throw.exception = FALSE) {
  
  rc <- TRUE
  first.exception <- NA
  
  
  # no results?
  if (is.null (results)) {

    logwarn ("[%s] No mccollect results",
             "mccollectResults")

    return (rc)
  }


  # iterate through the jobs
  loginfo ("[%s] Processing %s mccollect result(s) ...", 
           "mccollectResults", length (results))
  
  
  
  for (i in 1:length (results)) {
    
    name <- names (results) [i]
    result <- results [[i]]
    
    
    # did an exception occur?
    if (class (result) == "try-error") {
      
      logerror ("[%s] Job Name/PID: %s, Exception: %s", 
                "mccollectResults", 
                name, result [[1]])  
      
      # save the first exception
      if (is.na (first.exception)) {
        first.exception <- result
      }
      
      rc <- FALSE
      
    } else {
      
      loginfo ("[%s] Job Name/PID: %s, Result: %s", 
               "mccollectResults", 
               name, 
               ifelse (is.null (result), "NULL" , result))      
    }
    
  } # end result loop
  
  
  
  # throw an exception?
  if (throw.exception && !is.na (first.exception)) {
    stop (first.exception)
  }
  
  
  
  return (rc)
}
