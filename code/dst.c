
/*
* Function: DSTInitConn ()
*
* Description: Allocates the ODBC environment and calls SQLDriverConnect () 
* using the specified ODBC connection string. If the connection is successful 
* then TRUE is returned, otherwise, FALSE is returned.
*
* If the connection string uses RDBMS syntax instead of ODBC syntax then 
* Pro*C is used to connect to the target (if the executable is linked to the
* Pro*C libraries).
*
* If tracing is enabled for the DSN by the DST_TRACE_CMDS and DST_TRACE_DSNS 
* properties and the connection is a direct ODBC connection then tracing is 
* automatically configured via the the DSTInitTracing() function.
*
* In: pszConnStrIn
*
* Out: phenv, phdbc
*
* In/Out:
*
* Global properties:
*
*/
BOOL DSTInitConn (HENV *phenv, HDBC *phdbc,
                  const char *pszConnStrIn)
{

#ifdef DST_PRC

  if (DSTIsOraConnStr (pszConnStrIn))
    return PRCInitConn (phenv, phdbc, pszConnStrIn);

#endif

  {
    /* locals */
    BOOL bStatus = TRUE;


#ifdef DTP_H

    /* should XA calls be used in place of ODBC calls? */
    if (DSTUseXATxns ())
    {
      bStatus = DTPOpen (phenv, phdbc, pszConnStrIn, TMNOFLAGS);

      if (bStatus)
      {
        /* turn on autocommit to mimic ODBC behavior */
        DSTSetAutoCommit (*phdbc, SQL_AUTOCOMMIT_ON);
      }

      return bStatus;
    }

#endif


    /* report the call */
    {
      /* locals */
      SQLRETURN rc;
      DSTRING message, connStrIn, connName;
      char szConnStrOut [DST_CONN_STR_OUT_LENGTH] = "";

      DST_LOG_REPORT_ME ("DSTInitConn");

      /* validate the parameters */
      assert (pszConnStrIn != NULL && phenv != NULL && phdbc != NULL);

      /* init. dynamic strings */
      STRInit (&message, "");

      /* Configure the connection string to include ConnectionName=<dsn>. */
      /* This allows a connection to query the name of the DSN (via the */
      /* ttConfiguration() built-in) used to connect to the database. This */
      /* is required by some test functionality. */
      STRInit (&connName, "");
      STRInit (&connStrIn, pszConnStrIn);

      /* This will happen *only* if ConnectionName has not been defined */
      /* directly in the input connection string. */
      DSTGetConnStrAttr (pszConnStrIn, "ConnectionName", &connName);

      if (STRC_LENGTH (connName) == 0)
      {
        DSTGetConnStrAttr (pszConnStrIn, "DSN", &connName);
        DSTSetConnStrAttr (&connStrIn, "ConnectionName", STRC (connName));
      }


      /* allocate the environment - only if it has not already */
      /* been allocated by an existing ODBC connection */

      /* Note: SQLFreeEnv() is never called on the global ghenv variable */
      /* after the variable is initially allocated. */
      if (ghenv == SQL_NULL_HENV)
      {
        rc = SQLAllocEnv (&ghenv);

        /* did it succeed? */
        if (!DSTIsSuccess (rc))
        {
          DST_LOG_ERROR ("SQLAllocEnv () failed.");
          bStatus = FALSE;
        }
        else if (rc == SQL_SUCCESS_WITH_INFO)
        {
          DST_LOG_SQL_INFO (*phenv, SQL_NULL_HDBC, SQL_NULL_HSTMT,
            "SQLAllocEnv () info.");
        }
      }

      /* set the environment handle */
      if (bStatus)
      {
        assert (ghenv != SQL_NULL_HENV);
        *phenv = ghenv;
      }


      /* allocate the connection handle */
      if (bStatus)
      {
        rc = SQLAllocConnect (*phenv, phdbc);

        if (!DSTIsSuccess (rc))
        {
          DST_LOG_SQL_ERROR (*phenv, SQL_NULL_HDBC, SQL_NULL_HSTMT,
            "SQLAllocConnect () failed.");

          bStatus = FALSE;
        }
        else if (rc == SQL_SUCCESS_WITH_INFO)
        {
          DST_LOG_SQL_INFO (*phenv, *phdbc, SQL_NULL_HSTMT,
            "SQLAllocConnect () info.");
        }
      }



      /* connect */
      if (bStatus)
      {
        /* report the connection attempt */
        STRCopyCString (&message, "Connecting: %s");
        STRInsertCString (&message, pszConnStrIn);
        DST_LOG (STRC (message));

        /* connect to the data source */
        rc = SQLDriverConnect (*phdbc, NULL,
          (SQLCHAR*) STRC (connStrIn), SQL_NTS,
          (SQLCHAR*) szConnStrOut, DST_CONN_STR_OUT_LENGTH, NULL,
          SQL_DRIVER_NOPROMPT);


        /* was the connection successful? */
        if (!DSTIsSuccess (rc))
        {
          DST_LOG_SQL_ERROR (*phenv, *phdbc, SQL_NULL_HSTMT,
            "SQLDriverConnect () failed.");

          /* clean up - deallocate the connection handle */
          SQLFreeConnect (*phdbc);

          bStatus = FALSE;
        }
        else if (rc == SQL_SUCCESS_WITH_INFO)
        {
          DST_LOG_SQL_INFO (*phenv, *phdbc, SQL_NULL_HSTMT,
            "SQLDriverConnect () info.");
        }


        /* report the out connection string */
        if (bStatus)
        {
          STRCopyCString (&message, "Connection successful: %s");
          STRInsertCString (&message, szConnStrOut);
          DST_LOG (STRC (message));

          /* print the connected element ID if this is a grid connection */
          if (DSTIsGrid (*phdbc) && !DSTIsOraConn (*phdbc))
          {
            UDWORD iElementId = 0;
            
            if (DSTGetThisElementId (*phdbc, &iElementId))
            {
              STRCopyCString (&message, "Element ID: %u");
              STRInsertUnsignedInt (&message, iElementId);
              DST_LOG (STRC (message));
            }
          }


          /* track the connection */
          if (!DSTTrackConn (*phdbc, pszConnStrIn))
          {
            bStatus = FALSE;
            DSTFreeConn (*phenv, *phdbc);
          }

          /* enable tracing */
          if (bStatus)
            DSTInitTracing (*phdbc, pszConnStrIn);
        }
      }


      /* if the connection failed then be sure that the HDBC */
      /* is set to NULL */
      if (!bStatus)
      {
        *phdbc = SQL_NULL_HDBC;
      }
      else
      {
        /* report the handles */
        DSTLogODBCHandle ("HENV", *phenv);
        DSTLogODBCHandle ("HDBC", *phdbc);
      }


      /* free dynamic strings */
      STRFree (&connName);
      STRFree (&connStrIn);
      STRFree (&message);

      DST_LOG_UNREPORT_ME;
    }

    return bStatus;
  }
}


/*
* Function: DSTFreeConn ()
*
* Description: Calls SQLDisconnect () and deallocates the
* ODBC environment. If the disconnection is successful then
* TRUE is returned. If the connection is being tracked then
* DSTUntrackConn () is called automatically.
*
* In: henv, hdbc
*
* Out:
*
* In/Out:
*
*/
BOOL DSTFreeConn (HENV henv, HDBC hdbc)
{
  /* locals */
  SQLRETURN rc;
  BOOL bStatus = TRUE;

#ifdef DST_PRC
  if (DSTIsProCConn (hdbc))
    return PRCFreeConn (henv, hdbc);
#endif

#ifdef DTP_H

  /* should XA calls be used in place of ODBC calls? */
  if (DSTUseXATxns ())
  {

    /* if using XA commit any pending global txn before disconnecting */
    if (DSTIsTransactionPending (hdbc))
    {
      XID xid;
      DTPCreateXIDFromString (DST_XID_STRING, &xid);

      /* end, prepare and commit the current XA txn. */
      DTPEnd (&xid, hdbc, TMSUCCESS);

      if (DTPPrepare (&xid, hdbc, TMNOFLAGS) != XA_RDONLY)
        DTPCommit (&xid, hdbc, TMNOFLAGS);
    }

    bStatus = DTPClose (hdbc, TMNOFLAGS);
    return bStatus;
  }

#endif


  /* report the call */
  {
    DST_LOG_REPORT_ME ("DSTFreeConn");


    /* validate the parameters */
    assert (henv != SQL_NULL_HENV && hdbc != SQL_NULL_HDBC);
    DSTLogODBCHandle ("HENV", henv);
    DSTLogODBCHandle ("HDBC", hdbc);

    /* if a transaction is pending then roll it back */
    if (DSTIsTransactionPending (hdbc))
    {
      /* transaction rollback will fail with 'no logging' */
      /* connections - so try commit if rollback fails */
      if (!DSTIsSuccess (DSTTransact (hdbc, SQL_ROLLBACK)))
        DSTTransact (hdbc, SQL_COMMIT);
    }


    /* disconnect */
    rc = SQLDisconnect (hdbc);

    /* was it successful? */
    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (henv, hdbc, SQL_NULL_HSTMT,
        "SQLDisconnect () failed.");
      bStatus = FALSE;
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (henv, hdbc, SQL_NULL_HSTMT,
        "SQLDisconnect () info.");
    }


    /* free the allocated connection */
    rc = SQLFreeConnect (hdbc);

    /* was it successful? */
    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (henv, hdbc, SQL_NULL_HSTMT,
        "SQLFreeConnect () failed.");
      bStatus = FALSE;
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (henv, SQL_NULL_HDBC, SQL_NULL_HSTMT,
        "SQLFreeConnect () info.");
    }

    /* Note: SQLFreeEnv() is never called on the global ghenv variable after */
    /* the variable is initially allocated. */


    /* disable tracking? */
    if (DSTIsTrackedConn (hdbc))
      DSTUntrackConn (hdbc);


    DST_LOG_UNREPORT_ME;
  }

  return bStatus;
}
