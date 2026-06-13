// VCNThread.java



// IMPORTS
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Types;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Random;



public class VCNThread extends Thread
{
  
  // ATTRIBUTES
  private Connection m_conn = null;
  private VCNManager m_manager;

  private String m_threadName;
  private int m_threadId;
  private int m_txns;
  private boolean m_status;
  private Random m_rand;
  private TTTimer m_timer;

  private boolean m_ready = false;
  private boolean m_start = false;


  // this is a list of the name and number of all transactions and their 
  // probabilities
  LinkedHashMap<String, Integer> m_txnMap;
  LinkedHashMap<String, Integer> m_txnProbsMap;

  private CallableStatement m_stmt_del_nics_rg;
  private CallableStatement m_stmt_put_nics_rg;

  private CallableStatement m_stmt_del_ip_rg;
  private CallableStatement m_stmt_put_ip_rg;

  private CallableStatement m_stmt_del_lans_rg; 
  private CallableStatement m_stmt_put_lans_rg;

  private CallableStatement m_stmt_del_sec_lists_rg; 
  private CallableStatement m_stmt_put_sec_lists_rg;

  private CallableStatement m_stmt_del_vcns_rg;
  private CallableStatement m_stmt_put_vcns_rg;

  private CallableStatement m_stmt_del_nic_garp_rg;
  private CallableStatement m_stmt_put_nic_garp_rg;

  private CallableStatement m_stmt_del_nsg_assoc_rg;
  private CallableStatement m_stmt_put_nsg_assoc_rg;

  private CallableStatement m_stmt_del_float_pvt_ips_rg;
  private CallableStatement m_stmt_put_float_pvt_ips_rg;

  private CallableStatement m_stmt_del_float_pvt_ip_atts_rg;
  private CallableStatement m_stmt_put_float_pvt_ip_atts_rg;

  private CallableStatement m_stmt_del_float_ips_rg;
  private CallableStatement m_stmt_put_float_ips_rg;

  private CallableStatement m_stmt_put_cache_status;

  private CallableStatement m_stmt_del_float_ip_atts_rg;
  private CallableStatement m_stmt_put_float_ip_atts_rg;

  private CallableStatement m_stmt_del_subns_rg;
  private CallableStatement m_stmt_put_subns_rg;

  private CallableStatement m_stmt_del_net_sec_group_rg;
  private CallableStatement m_stmt_put_net_sec_group_rg;

  private CallableStatement m_stmt_query_get_nic;
  private CallableStatement m_stmt_query_get_net_sec_groups;
  private CallableStatement m_stmt_query_get_prim_pub_ip;
  private CallableStatement m_stmt_query_get_prim_priv_ip;

  private CallableStatement m_stmt_query_get_ip_addr_nic;

  // OPERATIONS
  
  // ----------------------------------------
  // VCNThread ()
  public VCNThread (VCNManager manager, int threadId, int txns,
    LinkedHashMap<String, Integer> txnMap,
    LinkedHashMap<String, Integer> txnProbsMap)
  {
    
    // initialize the thread name with the xid data
    super ("VCNThread");
    
    // initialize attributes
    m_manager = manager;
    m_threadId = threadId;
    m_txns = txns;

    m_threadName = "VCNThread " + m_threadId;
    m_rand = new Random ();

    m_timer = new TTTimer ();   


    // the transaction list and probabilties
    m_txnMap = txnMap;
    m_txnProbsMap = txnProbsMap;


    // the status of the branch is 'success' unless something
    // unexpected occurs
    m_status = true;
  
    // report the creation of this TxnBranch
    m_manager.log (m_threadName, "VCNThread created.");
    
    return;
  }
  

  
  // ----------------------------------------
  // checkoutConn ()
  // Checks a connection out from the VCNManager
  private void checkoutConn () throws SQLException
  {
    // checkout a connection
    do
    {        
      m_conn = m_manager.checkoutConn (m_threadName);

      // sleep before trying again
      if (m_conn == null)
      {
        try
        {
          this.sleep (25);
        }
        catch (java.lang.InterruptedException ex) 
        {
          m_status = false;
          m_manager.logErr (m_threadName, ex.getMessage ()); 
          break;
        }
      }
    }
    while (m_conn == null);


    // turn off autocommit for 2-safe
    m_conn.setAutoCommit (false);

    return;
  }
  
  
  // ----------------------------------------
  // checkinConn ()
  // Checks a connection back into the VCNManager
  private void checkinConn () throws SQLException
  {
    m_conn.setAutoCommit (true);
    m_manager.checkinConn (m_threadName, m_conn);    
    m_conn = null;
 
    return;
  }
  
  
  // ----------------------------------------
  // run ()
  // Execution of the VCNThread begins here
  public void run ()
  {
    
    // report the beginning of the thread
    m_manager.log (m_threadName, "Running...");
    
    // get the set of transaction codes
    Object[] codes = m_txnMap.values().toArray();
    
    // convert probabilites to double and convert the values to fractions
    Object [] objectProbs = m_txnProbsMap.values().toArray();
    double [] probs = new double [objectProbs.length];

    for (int index = 0; index < objectProbs.length; index++) 
    {
      probs[index] = Double.valueOf ((Integer) objectProbs[index]);
      probs[index] = probs[index] / 100;
    }        


    // execute PLSQL operations
    try
    {
      checkoutConn ();
      prepareCalls ();

      // indicate that the thread is ready to execute
      m_ready = true;

      // wait for the start flag
      m_manager.log (m_threadName, "Waiting to start ...");
      
      while (m_start == false)
      {
        Thread.sleep (1);
      }


      for (int iteration = 0; iteration < m_txns; iteration ++)
      {
        m_manager.log (m_threadName, "Iteration #" + iteration);



        // get the random transaction code
        int index = getRandomIndex (probs, m_rand);
        int txnCode = (int) codes [index];



        m_manager.log (m_threadName, "Executing transaction code " + 
          txnCode + " ...");

        try
        {

          switch (txnCode) 
          {
            // NICS_RG_DATA
            case 16:
              del_nics_rg ();
              break;
            
            case 26:
              put_nics_rg ();
              break;

            // IP_RG
            case 7:
              del_ip_rg ();
              break;

            case 14:
              put_ip_rg ();
              break;

            // VLANS_RG
            case 9:
              del_lans_rg ();
              break;
            
            case 22:
              put_lans_rg ();
              break;

            // SECURITY_LISTS_REGION
            case 11:
              del_sec_lists_rg ();
              break;
              
            case 21:
              put_sec_lists_rg ();
              break;

            // VCNS_RG
            case 4:
              del_vcns_rg ();
              break;

            case 18:
              put_vcns_rg ();
              break;

            // NIC_GARP_INFO_RG
            case 3:
              del_nic_garp_rg ();
              break;
            
            case 15:
              put_nic_garp_rg ();
              break;

            // NSG_ASSOC_RG
            case 12:
              del_nsg_assoc_rg ();
              break;

            case 13:  
              put_nsg_assoc_rg ();
              break;

            // FLOAT_PVT_IPS_RG
            case 6:
              del_float_pvt_ips_rg ();
              break;

            case 25:  
              put_float_pvt_ips_rg ();
              break;

            // FLOAT_PVT_IP_ATTS_RG
            case 5:
              del_float_pvt_ip_atts_rg ();
              break;

            case 17:
              put_float_pvt_ip_atts_rg ();
              break;

            // FLOAT_IPS_RG
            case 2:
              del_float_ips_rg ();
              break;

            case 20:
              put_float_ips_rg ();
              break;

            // CACHE_STATUS
            case 27:
              put_cache_status ();
              break;

            // FLOAT_IP_ATTS_RG
            case 8:
              del_float_ip_atts_rg ();
              break;
            
            case 19:  
              put_float_ip_atts_rg ();
              break;

            // SUBNETS_RG
            case 1:
              del_subns_rg ();
              break;

            case 24:  
              put_subns_rg ();
              break;

            // NET_SEC_GROUP_RG
            case 10:
              del_net_sec_group_rg ();
              break;
            
            case 23:
              put_net_sec_group_rg ();
              break;
            

            // queries
            case 31:
              query_get_nic ();
              break;

            case 30:
              query_get_net_sec_groups ();
              break;

            case 28:  
              query_get_prim_pub_ip ();
              break;

            case 29:  
              query_get_prim_priv_ip ();
              break;

            case 32:  
              query_get_ip_addr_nic ();
              break;

            default:
              throw new Exception ("Invalid transaction code");
          }

        }
        catch (SQLException ex)
        {
          m_manager.handleSQLException (m_threadName, ex);
          
          // if an error occurs in a transaction, we may need to rollback
          m_conn.rollback ();
        }


      } // end transaction loop




      checkinConn ();
    }
    catch (Exception ex)
    {
      // unexpected failure
      m_manager.handleException (m_threadName, ex);
      m_status = false;
    }
    finally
    {
      closeStatements ();

      if (m_conn != null)
      {
        try
        {
          checkinConn ();
        }
        catch (SQLException ex)
        {
          m_manager.handleSQLException (m_threadName, ex);
          m_status = false;
        }
      }
    }

    if (m_status == false)
      m_manager.logErr (m_threadName, "FAILED");
    else
      m_manager.log (m_threadName, "PASSED");
    
    return;
  }
  


  private void closeStatements ()
  {
    closeStatement (m_stmt_del_nics_rg);
    closeStatement (m_stmt_put_nics_rg);
    closeStatement (m_stmt_del_ip_rg);
    closeStatement (m_stmt_put_ip_rg);
    closeStatement (m_stmt_del_lans_rg);
    closeStatement (m_stmt_put_lans_rg);
    closeStatement (m_stmt_del_sec_lists_rg);
    closeStatement (m_stmt_put_sec_lists_rg);
    closeStatement (m_stmt_del_vcns_rg);
    closeStatement (m_stmt_put_vcns_rg);
    closeStatement (m_stmt_del_nic_garp_rg);
    closeStatement (m_stmt_put_nic_garp_rg);
    closeStatement (m_stmt_del_nsg_assoc_rg);
    closeStatement (m_stmt_put_nsg_assoc_rg);
    closeStatement (m_stmt_del_float_pvt_ips_rg);
    closeStatement (m_stmt_put_float_pvt_ips_rg);
    closeStatement (m_stmt_del_float_pvt_ip_atts_rg);
    closeStatement (m_stmt_put_float_pvt_ip_atts_rg);
    closeStatement (m_stmt_del_float_ips_rg);
    closeStatement (m_stmt_put_float_ips_rg);
    closeStatement (m_stmt_put_cache_status);
    closeStatement (m_stmt_del_float_ip_atts_rg);
    closeStatement (m_stmt_put_float_ip_atts_rg);
    closeStatement (m_stmt_del_subns_rg);
    closeStatement (m_stmt_put_subns_rg);
    closeStatement (m_stmt_del_net_sec_group_rg);
    closeStatement (m_stmt_put_net_sec_group_rg);
    closeStatement (m_stmt_query_get_nic);
    closeStatement (m_stmt_query_get_net_sec_groups);
    closeStatement (m_stmt_query_get_prim_pub_ip);
    closeStatement (m_stmt_query_get_prim_priv_ip);
    closeStatement (m_stmt_query_get_ip_addr_nic);

    m_stmt_del_nics_rg = null;
    m_stmt_put_nics_rg = null;
    m_stmt_del_ip_rg = null;
    m_stmt_put_ip_rg = null;
    m_stmt_del_lans_rg = null;
    m_stmt_put_lans_rg = null;
    m_stmt_del_sec_lists_rg = null;
    m_stmt_put_sec_lists_rg = null;
    m_stmt_del_vcns_rg = null;
    m_stmt_put_vcns_rg = null;
    m_stmt_del_nic_garp_rg = null;
    m_stmt_put_nic_garp_rg = null;
    m_stmt_del_nsg_assoc_rg = null;
    m_stmt_put_nsg_assoc_rg = null;
    m_stmt_del_float_pvt_ips_rg = null;
    m_stmt_put_float_pvt_ips_rg = null;
    m_stmt_del_float_pvt_ip_atts_rg = null;
    m_stmt_put_float_pvt_ip_atts_rg = null;
    m_stmt_del_float_ips_rg = null;
    m_stmt_put_float_ips_rg = null;
    m_stmt_put_cache_status = null;
    m_stmt_del_float_ip_atts_rg = null;
    m_stmt_put_float_ip_atts_rg = null;
    m_stmt_del_subns_rg = null;
    m_stmt_put_subns_rg = null;
    m_stmt_del_net_sec_group_rg = null;
    m_stmt_put_net_sec_group_rg = null;
    m_stmt_query_get_nic = null;
    m_stmt_query_get_net_sec_groups = null;
    m_stmt_query_get_prim_pub_ip = null;
    m_stmt_query_get_prim_priv_ip = null;
    m_stmt_query_get_ip_addr_nic = null;
  }


  private void closeStatement (PreparedStatement stmt)
  {
    if (stmt != null)
    {
      try
      {
        stmt.close ();
      }
      catch (SQLException ex)
      {
        m_manager.handleSQLException (m_threadName, ex);
        m_status = false;
      }
    }
  }



  // ----------------------------------------
  // prepareCalls ()
  private void prepareCalls () throws SQLException
  {
    
    m_stmt_del_nics_rg = m_conn.prepareCall
      ("BEGIN vcn.del_nics_rg (:lease_valid, :ls_id, :ls_fencing_token, :nic_id); END;");
    
    m_stmt_put_nics_rg = m_conn.prepareCall
      ("BEGIN vcn.put_nics_rg (:updated_rows, :ls_id, :ls_fencing_token, " + 
      ":nic_id, :nic_subn_id, :nic_comp_id, :nic_lan_id, " + 
      ":nic_public_ip, :nic_overlay_mac, :nic_resource_id, " + 
      ":nic_is_nic_service_nic, :nic_do_json); END;"); 
  

    m_stmt_del_ip_rg = m_conn.prepareCall
      ("BEGIN vcn.del_ip_rg (:lease_valid, :ls_id, :ls_fencing_token, :nic_id); END;");

    m_stmt_put_ip_rg = m_conn.prepareCall
      ("BEGIN vcn.put_ip_rg ( " + 
                "        :updated_rows, " + 
                "        :ls_id, " + 
                "        :ls_fencing_token, " + 
                "        :ip_id, " + 
                "        :ip_nic_id, " + 
                "        :ip_subn_id, " + 
                "        :ip_ip_address, " + 
                "        :ip_time_created, " + 
                "        :ip_do_json); END;");

    
    m_stmt_del_lans_rg = m_conn.prepareCall
      ("BEGIN vcn.del_lans_rg (:lease_valid, :ls_id, :ls_fencing_token, :lan_id); END;");

    m_stmt_put_lans_rg = m_conn.prepareCall
      ("BEGIN vcn.put_lans_rg (" + 
                      " :updated_rows, " +
                      " :ls_id, " +
                      " :ls_fencing_token, " +
                      " :lan_id, " +
                      " :lan_comp_id, " +
                      " :lan_vcn_id, " +
                      " :lan_rt_table_id, " +
                      " :lan_display_name, " +
                      " :lan_virtual_rtr_mac, " +
                      " :lan_time_created, " +
                      " :lan_do_json); END;");



    m_stmt_del_sec_lists_rg = m_conn.prepareCall
      ("BEGIN vcn.del_sec_lists_rg (:lease_valid, :ls_id, :ls_fencing_token, :sl_id); END;");


    m_stmt_put_sec_lists_rg = m_conn.prepareCall
      ("BEGIN  vcn.put_sec_lists_rg (" +
                "        :updated_rows, " +
                "        :ls_id, " +
                "        :ls_fencing_token,  " +
                "        :sl_id, " +
                "        :sl_comp_id, " +
                "        :sl_vcn_id,  " +
                "        :sl_display_name,  " +
                "        :sl_time_created,  " +
                "        :sl_do_json); END;");


    m_stmt_del_vcns_rg = m_conn.prepareCall
      ("BEGIN vcn.del_vcns_rg (:lease_valid, :ls_id, :ls_fencing_token, :vcn_id); END;");
          
    m_stmt_put_vcns_rg = m_conn.prepareCall
      ("BEGIN vcn.put_vcns_rg (" + 
                "        :updated_rows, " + 
                "        :ls_id, " + 
                "        :ls_fencing_token, " + 
                "        :vcn_id, " + 
                "        :default_rt_table_id, " + 
                "        :default_sec_list_id, " + 
                "        :default_dhcp_options_id, " + 
                "        :comp_id, " + 
                "        :display_name, " + 
                "        :time_created, " + 
                "        :vcn_do_json) ; END;");


    m_stmt_del_nic_garp_rg = m_conn.prepareCall 
      ("BEGIN vcn.del_nic_garp_info_rg (:lease_valid, :ls_id, :ls_fencing_token, :nic_id); END;");

    m_stmt_put_nic_garp_rg = m_conn.prepareCall 
      ("BEGIN vcn.put_nic_garp_info_rg (" + 
                "        :updated_rows, " + 
                "        :ls_id,  " + 
                "        :ls_fencing_token,  " + 
                "        :nic_id,  " + 
                "        :nic_garp_info_do_json) ; END;");



    m_stmt_del_nsg_assoc_rg = m_conn.prepareCall
      ("BEGIN vcn.del_nsg_associations_rg (:lease_valid, :ls_id, :ls_fencing_token, :nsg_id, :nic_id); END;");

    m_stmt_put_nsg_assoc_rg = m_conn.prepareCall
      ("BEGIN vcn.put_nsg_associations_rg (" + 
                "        :updated_rows, " +
                "        :ls_id, " +
                "        :ls_fencing_token, " +
                "        :nsg_id, " +
                "        :nic_id, " +
                "        :association_time, " +
                "        :nsg_assc_do_json)  ; END;");


    m_stmt_del_float_pvt_ips_rg = m_conn.prepareCall
        ("BEGIN vcn.del_float_pvt_ips_rg (" +
         "  :lease_valid, " +
         "  :ls_id, " +
         "  :ls_fencing_token, " +
         "  :fpip_id )  ; END;");

    m_stmt_put_float_pvt_ips_rg = m_conn.prepareCall
        ("BEGIN vcn.put_float_pvt_ips_rg ( " +
          ":updated_rows, " +
          ":ls_id, " +
          ":ls_fencing_token, " +
          ":fpip_id, " +
          ":fpip_subn_id, " +
          ":fpip_ip_address_int, " +
          ":fpip_time_created, " +
          ":fpip_display_name, " +
          ":fpip_lan_id, " +
          ":fpip_do_json); END;");



    m_stmt_del_float_pvt_ip_atts_rg = m_conn.prepareCall
      ("BEGIN vcn.del_float_pvt_ip_atts_rg (" +
                "        :lease_valid, " +
                "        :ls_id, " +
                "        :ls_fencing_token, " +
                "        :fpipatt_id); END;");

    m_stmt_put_float_pvt_ip_atts_rg = m_conn.prepareCall
      ("BEGIN vcn.put_float_pvt_ip_atts_rg (" + 
                "        :updated_rows, " +
                "        :ls_id, " +
                "        :ls_fencing_token, " +
                "        :fpipatt_id, " +
                "        :fpipatt_fpip_id, " +
                "        :fpipatt_nic_id, " +
                "        :fpipatt_nat_ip_addr, " +
                "        :fpiatt_lifecycle_state, " +
                "        :fpipatt_do_json); END;");


    m_stmt_del_float_ips_rg = m_conn.prepareCall
      ("BEGIN vcn.del_float_ips_rg (" +
                "        :lease_valid, " +
                "        :ls_id,  " +
                "        :ls_fencing_token,  " +
                "        :fip_id); END;");

    m_stmt_put_float_ips_rg = m_conn.prepareCall
      ("BEGIN vcn.put_float_ips_rg ( " +
          " :updated_rows, " +
          " :ls_id, " +
          " :ls_fencing_token, " +
          " :fip_id, " +
          " :fip_ip_addr, " +
          " :fip_comp_id, " +
          " :fip_scope, " +
          " :fip_availability_domain, " +
          " :fip_lifetime, " +
          " :fip_public_ip_pool_id, " +
          " :fip_do_json); END;");


    m_stmt_put_cache_status = m_conn.prepareCall
      ("BEGIN vcn.put_cache_status (" +
                "        :updated_rows, " +
                "        :ls_id, " +
                "        :ls_fencing_token, " +
                "        :cs_id, " +
                "        :cs_cursor, " +
                "        :cs_commit_txn_id, " +
                "        :cs_status, " +
                "        :cs_buckets); END;");          


    m_stmt_del_float_ip_atts_rg = m_conn.prepareCall 
      ("BEGIN vcn.del_float_ip_atts_rg (" +
                "        :lease_valid, " +
                "        :ls_id, " +
                "        :ls_fencing_token, " +
                "        :fipatt_id); END;");


    m_stmt_put_float_ip_atts_rg = m_conn.prepareCall 
      ("BEGIN vcn.put_float_ip_atts_rg (" +
      "   :updated_rows, " +
      "   :ls_id, " +
      "   :ls_fencing_token, " +
      "   :fipatt_id, " +
      "   :fipatt_nic_id, " +
      "   :fipatt_float_ip_id, " +
      "   :fipatt_float_private_ip_id, " +
      "   :fipatt_assigned_entity_id, " +
      "   :fipatt_lifecycle_state, " +
      "   :fipatt_do_json); END;");

    m_stmt_del_subns_rg = m_conn.prepareCall 
      ("BEGIN vcn.del_subns_rg ( " +
      " :lease_valid, " +
      " :ls_id, " +
      " :ls_fencing_token, " +
      " :sn_id)    ; END;"); 

    m_stmt_put_subns_rg = m_conn.prepareCall
      ("BEGIN vcn.put_subns_rg (" +
          ":updated_rows, " +
          ":ls_id, " +
          ":ls_fencing_token, " +
          ":sn_id, " +
          ":sn_comp_id, " +
          ":sn_vcn_id, " +
          ":sn_rt_table_id, " +
          ":sn_dhcp_options_id, " +
          ":sn_dns_label, " +
          ":sn_display_name, " +
          ":sn_time_created, " +
          ":sn_do_json)   ; END;");


    m_stmt_del_net_sec_group_rg = m_conn.prepareCall
      ("BEGIN vcn.del_net_sec_group_rg (" +
                "        :lease_valid, " +
                "        :ls_id, " +
                "        :ls_fencing_token, " +
                "        :nsg_id); END;");

    m_stmt_put_net_sec_group_rg = m_conn.prepareCall
      ("BEGIN vcn.put_net_sec_group_rg (" +
                "        :updated_rows, " +
                "        :ls_id, " +
                "        :ls_fencing_token, " +
                "        :nsg_id, " +
                "        :nsg_dp_id, " +
                "        :nsg_comp_id, " +
                "        :nsg_vcn_id, " +
                "        :nsg_display_name, " +
                "        :nsg_time_created, " +
                "        :nsg_do_json); END;"); 
    

    m_stmt_query_get_nic = m_conn.prepareCall
      ("SELECT do_json FROM NICS_RG WHERE id = ?");

    m_stmt_query_get_net_sec_groups = m_conn.prepareCall
      ("SELECT NSG.do_json FROM NET_SEC_GROUP_RG NSG " + 
                "INNER JOIN NSG_ASSOC_RG NSG_ASC " + 
                "ON NSG.id = NSG_ASC.nsg_id " + 
                "WHERE NSG_ASC.nic_id = ?");


    m_stmt_query_get_prim_pub_ip = m_conn.prepareCall
      ("SELECT FIPs.do_json, FIPAs.do_json FROM FLOAT_IP_ATTS_RG FIPAs " +
        "INNER JOIN FLOAT_IPS_RG FIPs " +
        "ON FIPAs.float_ip_id = FIPs.id " +
        "WHERE FIPAs.float_private_ip_id = ? " +
        "AND FIPAs.lifecycle_state IN ('AVAILABLE', 'PROVISIONING')"); 

    m_stmt_query_get_prim_priv_ip = m_conn.prepareCall
        ("SELECT FPIPs.do_json, FPIPAs.do_json FROM FLOAT_PVT_IP_ATTS_RG FPIPAs " +
          "INNER JOIN FLOAT_PVT_IPS_RG FPIPs " +
          "ON FPIPAs.float_private_ip_id = FPIPs.id " +
          "WHERE FPIPAs.nic_id = ? " +
          "AND FPIPAs.lifecycle_state IN ('AVAILABLE', 'PROVISIONING')");


    m_stmt_query_get_ip_addr_nic =  m_conn.prepareCall
      ("SELECT do_json, time_created FROM VCN.IP_RG " +
        "WHERE nic_id = ? " +
        "AND rownum <= 32 " +
        "ORDER BY time_created ASC");
  

    return;
  }


  
  // ----------------------------------------
  // del_nics_rg ()
  private void del_nics_rg () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object nic_id = "0";

    
    m_manager.log (m_threadName, "Preparing del_nics_rg input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random id from NICS_RG_DATA
    nic_id = m_manager.getCachedValue (m_threadName, "NICS_RG_DATA", "ID");

   



    m_manager.log (m_threadName, "Calling del_nics_rg ...");

    // Prepare to call PLSQL:  
    // "BEGIN vcn.del_nics_rg (:lease_valid, :ls_id, :ls_fencing_token, :nic_id); END;"
      
    m_timer.start ();

    m_stmt_del_nics_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_nics_rg.setObject (2, ls_id);
    m_stmt_del_nics_rg.setObject (3, ls_fencing_token);
    m_stmt_del_nics_rg.setObject (4, nic_id);

    m_stmt_del_nics_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_nics_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_nics_rg.getInt (1);

    // done
    m_conn.commit ();

    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_nics_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_nics_rg", m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_nics_rg lease valid: " + lease_valid); 

    return;
  }
  
  
  // ----------------------------------------
  // put_nics_rg ()
  private void put_nics_rg () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    HashMap<String, Object> row;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    Object nic_id = "0"; 
    Object nic_subn_id = "0"; 
    Object nic_comp_id = "0";
    Object nic_lan_id = "0"; 
    Object nic_public_ip = "0"; 
    Object nic_overlay_mac = 0; 
    Object nic_resource_id = "0"; 
    Object nic_is_nic_service_nic = 0;
    Object nic_do_json = "0"; 



    m_manager.log (m_threadName, "Preparing put_nics_rg input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from NICS_RG_DATA
    row = m_manager.getCachedRow (m_threadName, "NICS_RG_DATA");

    nic_id = row.get ("ID"); 
    nic_comp_id = row.get ("COMPARTMENT_ID");
    nic_subn_id = row.get ("SUBNET_ID"); 
    nic_lan_id = row.get ("VLAN_ID"); 
    nic_public_ip = row.get ("PUBLIC_IP"); 
    nic_overlay_mac = row.get ("OVERLAY_MAC"); 
    nic_resource_id = row.get ("RESOURCE_ID"); 
    nic_is_nic_service_nic = row.get ("IS_NIC_SERVICE_NIC");
    nic_do_json = row.get ("DO_JSON");






    m_manager.log (m_threadName, "Calling put_nics_rg ...");

    // Prepare to call PLSQL: 
    //   "BEGIN vcn.put_nics_rg (:updated_rows, :ls_id, :ls_fencing_token, " + 
    //   ":nic_id, :nic_subn_id, :nic_comp_id, :nic_lan_id, " + 
    //   ":nic_public_ip, :nic_overlay_mac, :nic_resource_id, " + 
    //   ":nic_is_nic_service_nic, :nic_do_json); END;"

    m_timer.start ();

    m_stmt_put_nics_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_nics_rg.setObject (2, ls_id);
    m_stmt_put_nics_rg.setObject (3, ls_fencing_token);

    m_stmt_put_nics_rg.setObject (4, nic_id);
    m_stmt_put_nics_rg.setObject (5, nic_subn_id);
    m_stmt_put_nics_rg.setObject (6, nic_comp_id);
    m_stmt_put_nics_rg.setObject (7, nic_lan_id);

    m_stmt_put_nics_rg.setObject (8, nic_public_ip);
    m_stmt_put_nics_rg.setObject (9, nic_overlay_mac);
    m_stmt_put_nics_rg.setObject (10, nic_resource_id);

    m_stmt_put_nics_rg.setObject (11, nic_is_nic_service_nic);
    m_stmt_put_nics_rg.setObject (12, nic_do_json);


    m_stmt_put_nics_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_nics_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_nics_rg.getInt (1);

    // done
    m_conn.commit ();

    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_nics_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_nics_rg", m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_nics_rg updated_rows: " + updated_rows); 



    return;
  }


  // ----------------------------------------
  // del_ip_rg ()
  private void del_ip_rg () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object nic_id = "0";

    
    m_manager.log (m_threadName, "Preparing del_ip_rg input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

    // select a random ID from IP_RG_DATA
    nic_id = m_manager.getCachedValue (m_threadName, "IP_RG_DATA", "ID");
   



    m_manager.log (m_threadName, "Calling del_ip_rg ...");

    // Prepare to call PLSQL:  
    // "BEGIN vcn.del_ip_rg (:lease_valid, :ls_id, :ls_fencing_token, :nic_id); END;"
      
    m_timer.start ();

    m_stmt_del_ip_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_ip_rg.setObject (2, ls_id);
    m_stmt_del_ip_rg.setObject (3, ls_fencing_token);
    m_stmt_del_ip_rg.setObject (4, nic_id);

    m_stmt_del_ip_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_ip_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_ip_rg.getInt (1);

    // done
    m_conn.commit ();

    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_ip_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_ip_rg", m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_ip_rg lease valid: " + lease_valid); 

    return;
  }
  


  // ----------------------------------------
  // put_ip_rg ()
  private void put_ip_rg () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    HashMap<String, Object> row;

    Object ls_id = "0";
    Object ls_fencing_token = 0;
   
    Object ip_id = "0";  
    Object ip_nic_id = "0";
    Object ip_subn_id = "0";
    Object ip_ip_address = "0"; 
    Object ip_time_created = null; 
    Object ip_do_json = "0"; 



    m_manager.log (m_threadName, "Preparing put_ip_rg input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from IP_RG_DATA
    row = m_manager.getCachedRow (m_threadName, "IP_RG_DATA");

    ip_id = row.get ("ID"); 
    ip_nic_id = row.get ("NIC_ID");
    ip_subn_id = row.get ("SUBNET_ID");
    ip_ip_address = row.get ("IP_ADDRESS");
    ip_time_created = row.get ("TIME_CREATED"); 
    ip_do_json = row.get ("DO_JSON");


    m_manager.log (m_threadName, "Calling put_ip_rg ...");

    // Prepare to call PLSQL: 
    //   "BEGIN vcn.put_ip_rg ( " + 
    //"        :updated_rows, " + 
    //"        :ls_id " + 
    //"        :ls_fencing_token " + 
    //"        :ip_id " + 
    //"        :ip_nic_id " + 
    //"        :ip_subn_id " + 
    //"        :ip_ip_address " + 
    //"        :ip_time_created " + 
    //"        :ip_do_json); END;"

    m_timer.start ();

    m_stmt_put_ip_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_ip_rg.setObject (2, ls_id);
    m_stmt_put_ip_rg.setObject (3, ls_fencing_token);

    m_stmt_put_ip_rg.setObject (4, ip_id);
    m_stmt_put_ip_rg.setObject (5, ip_nic_id);
    m_stmt_put_ip_rg.setObject (6, ip_subn_id);
    m_stmt_put_ip_rg.setObject (7, ip_ip_address);
    m_stmt_put_ip_rg.setObject (8, ip_time_created);
    m_stmt_put_ip_rg.setObject (9, ip_do_json);


    m_stmt_put_ip_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_ip_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_ip_rg.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_ip_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_ip_rg", m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_ip_rg updated_rows: " + updated_rows); 



    return;
  }


  // ----------------------------------------
  // del_lans_rg ()
  private void del_lans_rg () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object lan_id = "0";

    
    m_manager.log (m_threadName, "Preparing del_lans_rg input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

    // select a random ID from VLANS_RG_DATA
    lan_id = m_manager.getCachedValue (m_threadName, "VLANS_RG_DATA", "ID");
   



    m_manager.log (m_threadName, "Calling del_lans_rg ...");

    // Prepare to call PLSQL:  
    // "BEGIN vcn.del_lans_rg (:lease_valid, :ls_id, :ls_fencing_token, :lan_id); END;"
      
    m_timer.start ();

    m_stmt_del_lans_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_lans_rg.setObject (2, ls_id);
    m_stmt_del_lans_rg.setObject (3, ls_fencing_token);
    m_stmt_del_lans_rg.setObject (4, lan_id);

    m_stmt_del_lans_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_lans_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_lans_rg.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_lans_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_lans_rg", m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_lans_rg lease valid: " + lease_valid); 

    return;
  }


  // ----------------------------------------
  // put_lans_rg ()
  private void put_lans_rg () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    Object lan_id = "0";
    Object lan_comp_id = "0";
    Object lan_vcn_id = "0";
    Object lan_rt_table_id = "0";
    Object lan_display_name = "0";
    Object lan_virtual_rtr_mac = 0;
    Object lan_time_created = null;
    Object lan_do_json = "0";



    m_manager.log (m_threadName, "Preparing put_lans_rg input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from VLANS_RG_DATA
    row = m_manager.getCachedRow (m_threadName, "VLANS_RG_DATA");

    lan_id = row.get ("ID"); 
    lan_comp_id = row.get ("COMPARTMENT_ID");
    lan_vcn_id = row.get ("VCN_ID");
    lan_rt_table_id = row.get ("ROUTE_TABLE_ID");
    lan_display_name = row.get ("DISPLAY_NAME");
    lan_virtual_rtr_mac = row.get ("VIRTUAL_ROUTER_MAC");
    lan_time_created = row.get ("TIME_CREATED"); 
    lan_do_json = row.get ("DO_JSON");





    m_manager.log (m_threadName, "Calling put_lans_rg ...");

    // Prepare to call PLSQL: 
    // "BEGIN vcn.put_lans_rg (" + 
    //                   " :updated_rows, " +
    //                   " :ls_id, " +
    //                   " :ls_fencing_token, " +
    //                   " :lan_id, " +
    //                   " :lan_comp_id, " +
    //                   " :lan_vcn_id, " +
    //                   " :lan_rt_table_id, " +
    //                   " :lan_display_name, " +
    //                   " :lan_virtual_rtr_mac, " +
    //                   " :lan_time_created, " +
    //                   " :lan_do_json); END;"

    m_timer.start ();

    m_stmt_put_lans_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_lans_rg.setObject (2, ls_id);
    m_stmt_put_lans_rg.setObject (3, ls_fencing_token);

    m_stmt_put_lans_rg.setObject (4, lan_id);
    m_stmt_put_lans_rg.setObject (5, lan_comp_id);
    m_stmt_put_lans_rg.setObject (6, lan_vcn_id);

    m_stmt_put_lans_rg.setObject (7, lan_rt_table_id);
    m_stmt_put_lans_rg.setObject (8, lan_display_name);
    m_stmt_put_lans_rg.setObject (9, lan_virtual_rtr_mac);

    m_stmt_put_lans_rg.setObject (10, lan_time_created);
    m_stmt_put_lans_rg.setObject (11, lan_do_json);

    m_stmt_put_lans_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_lans_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_lans_rg.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_lans_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_lans_rg", m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_lans_rg updated_rows: " + updated_rows); 



    return;
  }



  // ----------------------------------------
  // del_sec_lists_rg ()
  private void del_sec_lists_rg () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object sl_id = "0";

    
    m_manager.log (m_threadName, "Preparing del_sec_lists_rg input ...");
    


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");
 
    // select a random ID from SECURITY_LISTS_RG_DATA
    sl_id = m_manager.getCachedValue (m_threadName, "SECURITY_LISTS_RG_DATA", "ID");



    m_manager.log (m_threadName, "Calling del_sec_lists_rg ...");

    // Prepare to call PLSQL:  
    // "BEGIN vcn.del_sec_lists_rg (:lease_valid, :ls_id, :ls_fencing_token, :sl_id); END;"
      
    m_timer.start ();

    m_stmt_del_sec_lists_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_sec_lists_rg.setObject (2, ls_id);
    m_stmt_del_sec_lists_rg.setObject (3, ls_fencing_token);
    m_stmt_del_sec_lists_rg.setObject (4, sl_id);

    m_stmt_del_sec_lists_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_sec_lists_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_sec_lists_rg.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_sec_lists_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_sec_lists_rg", 
      m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_sec_lists_rg lease valid: " + 
      lease_valid); 

    return;
  }


  // ----------------------------------------
  // put_sec_lists_rg ()
  private void put_sec_lists_rg () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN put_sec_lists_rg (" +
    //             "        :updated_rows, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token  " +
    //             "        :sl_id, " +
    //             "        :sl_comp_id, " +
    //             "        :sl_vcn_id,  " +
    //             "        :sl_display_name,  " +
    //             "        :sl_time_created,  " +
    //             "        :sl_do_json); END;"

    Object sl_id = "0";
    Object sl_comp_id = "0";
    Object sl_vcn_id = "0";
    Object sl_display_name = "0";
    Object sl_time_created = null;
    Object sl_do_json = "0";




    m_manager.log (m_threadName, "Preparing put_sec_lists_rg input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");



    // select a random row from SECURITY_LISTS_RG_DATA
    row = m_manager.getCachedRow (m_threadName, "SECURITY_LISTS_RG_DATA");

    sl_id = row.get ("ID"); 
    sl_comp_id = row.get ("COMPARTMENT_ID");
    sl_vcn_id = row.get ("VCN_ID");
    sl_display_name = row.get ("DISPLAY_NAME");
    sl_time_created = row.get ("TIME_CREATED"); 
    sl_do_json = row.get ("DO_JSON");



    m_manager.log (m_threadName, "Calling put_sec_lists_rg ...");

    // Prepare to call PLSQL: 
    // "BEGIN put_sec_lists_rg (" +
    //             "        :updated_rows, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token  " +
    //             "        :sl_id, " +
    //             "        :sl_comp_id, " +
    //             "        :sl_vcn_id,  " +
    //             "        :sl_display_name,  " +
    //             "        :sl_time_created,  " +
    //             "        :sl_do_json); END;"


    m_timer.start ();

    m_stmt_put_sec_lists_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_sec_lists_rg.setObject (2, ls_id);
    m_stmt_put_sec_lists_rg.setObject (3, ls_fencing_token);

    m_stmt_put_sec_lists_rg.setObject (4, sl_id);
    m_stmt_put_sec_lists_rg.setObject (5, sl_comp_id);
    m_stmt_put_sec_lists_rg.setObject (6, sl_vcn_id);

    m_stmt_put_sec_lists_rg.setObject (7, sl_display_name);

    m_stmt_put_sec_lists_rg.setObject (8, sl_time_created);

    // DEBUG:
    m_stmt_put_sec_lists_rg.setObject (9, sl_do_json);

    // m_stmt_put_sec_lists_rg.setString (9, sl_do_json.toString ());

    // Clob clob = m_conn.createClob();
    // clob.setString (1, sl_do_json.toString());
    // m_stmt_put_sec_lists_rg.setClob (9, clob);


    m_stmt_put_sec_lists_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_sec_lists_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_sec_lists_rg.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_sec_lists_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_sec_lists_rg", m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_sec_lists_rg updated_rows: " + updated_rows); 



    return;
  }


  // ----------------------------------------
  // del_vcns_rg ()
  private void del_vcns_rg () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object vcn_id = "0";

    
    m_manager.log (m_threadName, "Preparing del_vcns_rg input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random ID from VCNS_RG_DATA
    vcn_id = m_manager.getCachedValue (m_threadName, "VCNS_RG_DATA", "ID");

   
    m_manager.log (m_threadName, "Calling del_vcns_rg ...");

    // Prepare to call PLSQL:  
    // "BEGIN vcn.del_vcns_rg (:lease_valid, :ls_id, :ls_fencing_token, :vcn_id); END;"
      
    m_timer.start ();

    m_stmt_del_vcns_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_vcns_rg.setObject (2, ls_id);
    m_stmt_del_vcns_rg.setObject (3, ls_fencing_token);
    m_stmt_del_vcns_rg.setObject (4, vcn_id);

    m_stmt_del_vcns_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_vcns_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_vcns_rg.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_vcns_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_vcns_rg", 
      m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_vcns_rg lease valid: " + 
      lease_valid); 

    return;
  }

  // ----------------------------------------
  // put_vcns_rg ()
  private void put_vcns_rg () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN put_vcns_rg (" + 
    //             "        updated_rows, " + 
    //             "        ls_id, " + 
    //             "        ls_fencing_token, " + 
    //             "        vcn_id, " + 
    //             "        default_rt_table_id, " + 
    //             "        default_sec_list_id, " + 
    //             "        default_dhcp_options_id, " + 
    //             "        comp_id, " + 
    //             "        display_name, " + 
    //             "        time_created, " + 
    //             "        vcn_do_json) ; END;"

    Object vcn_id = "0";
    Object default_rt_table_id = "0";
    Object default_sec_list_id = "0";
    Object default_dhcp_options_id = "0";
    Object comp_id = "0";
    Object display_name = "0";
    Object time_created = null;
    Object vcn_do_json = "0";



    m_manager.log (m_threadName, "Preparing put_vcns_rg input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from VCNS_RG_DATA
     row = m_manager.getCachedRow (m_threadName, "VCNS_RG_DATA");

     vcn_id = row.get ("ID"); 
     default_rt_table_id = row.get ("DEFAULT_ROUTE_TABLE_ID"); 
     default_sec_list_id = row.get ("DEFAULT_SECURITY_LIST_ID"); 
     default_dhcp_options_id = row.get ("DEFAULT_DHCP_OPTIONS_ID"); 
     comp_id = row.get ("COMPARTMENT_ID"); 
     display_name = row.get ("DISPLAY_NAME"); 
     time_created = row.get ("TIME_CREATED");  
     vcn_do_json = row.get ("DO_JSON");     



    m_manager.log (m_threadName, "Calling put_vcns_rg ...");

    // "BEGIN put_vcns_rg (" + 
    //             "        updated_rows, " + 
    //             "        ls_id, " + 
    //             "        ls_fencing_token, " + 
    //             "        vcn_id, " + 
    //             "        default_rt_table_id, " + 
    //             "        default_sec_list_id, " + 
    //             "        default_dhcp_options_id, " + 
    //             "        comp_id, " + 
    //             "        display_name, " + 
    //             "        time_created, " + 
    //             "        vcn_do_json) ; END;"


    m_timer.start ();

    m_stmt_put_vcns_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_vcns_rg.setObject (2, ls_id);
    m_stmt_put_vcns_rg.setObject (3, ls_fencing_token);

    m_stmt_put_vcns_rg.setObject (4, vcn_id);
    m_stmt_put_vcns_rg.setObject (5, default_rt_table_id);
    m_stmt_put_vcns_rg.setObject (6, default_sec_list_id);
    m_stmt_put_vcns_rg.setObject (7, default_dhcp_options_id);

    m_stmt_put_vcns_rg.setObject (8, comp_id);
    m_stmt_put_vcns_rg.setObject (9, display_name);

    m_stmt_put_vcns_rg.setObject (10, time_created);
    m_stmt_put_vcns_rg.setObject (11, vcn_do_json);

    m_stmt_put_vcns_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_vcns_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_vcns_rg.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_vcns_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_vcns_rg", m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_vcns_rg updated_rows: " + updated_rows); 



    return;
  }



  // ----------------------------------------
  // del_nic_garp_rg ()
  private void del_nic_garp_rg () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object nic_id = "0";

    
    m_manager.log (m_threadName, "Preparing del_nic_garp_info_rg input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random ID from NIC_GARP_INFO_RG_DATA
    nic_id = m_manager.getCachedValue(m_threadName, "NIC_GARP_INFO_RG_DATA", "ID");
   



    m_manager.log (m_threadName, "Calling del_nic_garp_info_rg ...");

    // Prepare to call PLSQL:  
    // "BEGIN vcn.del_nic_garp_info_rg (:lease_valid, :ls_id, :ls_fencing_token, :nic_id); END;"
      
    m_timer.start ();

    m_stmt_del_nic_garp_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_nic_garp_rg.setObject (2, ls_id);
    m_stmt_del_nic_garp_rg.setObject (3, ls_fencing_token);
    m_stmt_del_nic_garp_rg.setObject (4, nic_id);

    m_stmt_del_nic_garp_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_nic_garp_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_nic_garp_rg.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_nic_garp_info_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_nic_garp_info_rg", 
      m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_nic_garp_info_rg lease valid: " + 
      lease_valid); 

    return;
  }

  // ----------------------------------------
  // put_nic_garp_rg ()
  private void put_nic_garp_rg () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN put_nic_garp_info_rg " + 
    //             "        :updated_rows, " + 
    //             "        :ls_id,  " + 
    //             "        :ls_fencing_token,  " + 
    //             "        :nic_id,  " + 
    //             "        :nic_garp_info_do_json ; END;"

    Object nic_id = "0";
    Object nic_garp_info_do_json = "0";



    m_manager.log (m_threadName, "Preparing put_nic_garp_info_rg input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from NIC_GARP_INFO_RG_DATA
    row = m_manager.getCachedRow (m_threadName, "NIC_GARP_INFO_RG_DATA");
    nic_id = row.get ("ID"); 
    nic_garp_info_do_json = row.get ("DO_JSON");    




    m_manager.log (m_threadName, "Calling put_nic_garp_info_rg ...");

    // "BEGIN put_nic_garp_info_rg " + 
    //             "        :updated_rows, " + 
    //             "        :ls_id,  " + 
    //             "        :ls_fencing_token,  " + 
    //             "        :nic_id,  " + 
    //             "        :nic_garp_info_do_json ; END;"



    m_timer.start ();

    m_stmt_put_nic_garp_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_nic_garp_rg.setObject (2, ls_id);
    m_stmt_put_nic_garp_rg.setObject (3, ls_fencing_token);

    m_stmt_put_nic_garp_rg.setObject (4, nic_id);
    m_stmt_put_nic_garp_rg.setObject (5, nic_garp_info_do_json);

    m_stmt_put_nic_garp_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_nic_garp_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_nic_garp_rg.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_nic_garp_info_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_nic_garp_info_rg", m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_nic_garp_info_rg updated_rows: " + updated_rows); 



    return;
  }




  // ----------------------------------------
  // del_nsg_assoc_rg ()
  private void del_nsg_assoc_rg () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;

    Object nsg_id = "0";
    Object nic_id = "0";

    
    m_manager.log (m_threadName, "Preparing del_nsg_associations_rg input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select random row from NSG_ASSOC_RG_DATA
    row = m_manager.getCachedRow (m_threadName, "NSG_ASSOC_RG_DATA");
    nsg_id = row.get ("NSG_ID");
    nic_id = row.get ("NIC_ID"); 



    m_manager.log (m_threadName, "Calling del_nsg_associations_rg ...");

    // Prepare to call PLSQL:  
    // "BEGIN vcn.del_nsg_associations_rg (:lease_valid, :ls_id, :ls_fencing_token, :nsg_id, :nic_id); END;"
      
    m_timer.start ();

    m_stmt_del_nsg_assoc_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_nsg_assoc_rg.setObject (2, ls_id);
    m_stmt_del_nsg_assoc_rg.setObject (3, ls_fencing_token);
    m_stmt_del_nsg_assoc_rg.setObject (4, nsg_id);
    m_stmt_del_nsg_assoc_rg.setObject (5, nic_id);

    m_stmt_del_nsg_assoc_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_nsg_assoc_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_nsg_assoc_rg.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_nsg_associations_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_nsg_associations_rg", 
      m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_nsg_associations_rg lease valid: " + 
      lease_valid); 

    return;
  }


  // ----------------------------------------
  // put_nsg_assoc_rg ()
  private void put_nsg_assoc_rg () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN put_nsg_associations_rg (" + //
    //             "        :updated_rows, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :nsg_id, " +
    //             "        :nic_id, " +
    //             "        :association_time, " +
    //             "        :nsg_assc_do_json)  ; END;"

    Object nsg_id = "0";
    Object nic_id = "0";
    Object association_time = null;
    Object nsg_assc_do_json = "0";



    m_manager.log (m_threadName, "Preparing put_nsg_associations_rg input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");



    // select a random row from NSG_ASSOC_RG_DATA
    row = m_manager.getCachedRow (m_threadName, "NSG_ASSOC_RG_DATA");
    nsg_id = row.get ("NSG_ID"); 
    nic_id = row.get ("NIC_ID"); 
    association_time = row.get ("ASSOCIATION_TIME"); 
    nsg_assc_do_json = row.get ("DO_JSON");  



    m_manager.log (m_threadName, "Calling put_nsg_associations_rg ...");

    // "BEGIN put_nsg_associations_rg (" + //
    //             "        :updated_rows, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :nsg_id, " +
    //             "        :nic_id, " +
    //             "        :association_time, " +
    //             "        :nsg_assc_do_json)  ; END;"



    m_timer.start ();

    m_stmt_put_nsg_assoc_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_nsg_assoc_rg.setObject (2, ls_id);
    m_stmt_put_nsg_assoc_rg.setObject (3, ls_fencing_token);

    m_stmt_put_nsg_assoc_rg.setObject (4, nsg_id);
    m_stmt_put_nsg_assoc_rg.setObject (5, nic_id);

    m_stmt_put_nsg_assoc_rg.setObject (6, association_time);    
    m_stmt_put_nsg_assoc_rg.setObject (7, nsg_assc_do_json);


    m_stmt_put_nsg_assoc_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_nsg_assoc_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_nsg_assoc_rg.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_nsg_associations_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_nsg_associations_rg", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_nsg_associations_rg updated_rows: " + 
      updated_rows); 



    return;
  }



  // ----------------------------------------
  // del_float_pvt_ips_rg ()
  private void del_float_pvt_ips_rg () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;

    Object fpip_id = "0";


    
    m_manager.log (m_threadName, "Preparing del_float_pvt_ips_rg input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random ID from FLOAT_PVT_IPS_RG_DATA
    fpip_id = m_manager.getCachedValue (m_threadName, "FLOAT_PVT_IPS_RG_DATA", "ID");
   




    m_manager.log (m_threadName, "Calling del_float_pvt_ips_rg ...");

    // Prepare to call PLSQL:  
    // "BEGIN vcn.del_float_pvt_ips_rg (" +
    //      "  :lease_valid, " +
    //      "  :ls_id, " +
    //      "  :ls_fencing_token, " +
    //      "  :fpip_id )  ; END;"

    m_timer.start ();

    m_stmt_del_float_pvt_ips_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_float_pvt_ips_rg.setObject (2, ls_id);
    m_stmt_del_float_pvt_ips_rg.setObject (3, ls_fencing_token);
    m_stmt_del_float_pvt_ips_rg.setObject (4, fpip_id);

    m_stmt_del_float_pvt_ips_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_float_pvt_ips_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_float_pvt_ips_rg.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_float_pvt_ips_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_float_pvt_ips_rg", 
      m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_float_pvt_ips_rg lease valid: " + 
      lease_valid); 

    return;
  }



  // ----------------------------------------
  // put_float_pvt_ips_rg ()
  private void put_float_pvt_ips_rg () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN put_float_pvt_ips_rg ( " +
    //       ":updated_rows, " +
    //       ":ls_id, " +
    //       ":ls_fencing_token, " +
    //       ":fpip_id, " +
    //       ":fpip_subn_id, " +
    //       ":fpip_ip_address_int, " +
    //       ":fpip_time_created, " +
    //       ":fpip_display_name, " +
    //       ":fpip_lan_id, " +
    //       ":fpip_do_json); END;"

    Object fpip_id = "0";
    Object fpip_subn_id = "0";
    Object fpip_ip_address_int = 0;
    Object fpip_time_created = null;
    Object fpip_display_name = "0";
    Object fpip_lan_id = "0";
    Object fpip_do_json = "0";



    m_manager.log (m_threadName, "Preparing put_float_pvt_ips_rg input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from FLOAT_PVT_IPS_RG_DATA
    row = m_manager.getCachedRow (m_threadName, "FLOAT_PVT_IPS_RG_DATA");

    fpip_id = row.get ("ID"); 
    fpip_subn_id = row.get ("SUBNET_ID"); 
    fpip_ip_address_int = row.get ("IP_ADDRESS_INT"); 
    fpip_time_created = row.get ("TIME_CREATED"); 
    fpip_display_name = row.get ("DISPLAY_NAME"); 
    fpip_lan_id = row.get ("VLAN_ID"); 
    fpip_do_json = row.get ("DO_JSON");      



    m_manager.log (m_threadName, "Calling put_float_pvt_ips_rg ...");

    // "BEGIN put_float_pvt_ips_rg ( " +
    //       ":updated_rows, " +
    //       ":ls_id, " +
    //       ":ls_fencing_token, " +
    //       ":fpip_id, " +
    //       ":fpip_subn_id, " +
    //       ":fpip_ip_address_int, " +
    //       ":fpip_time_created, " +
    //       ":fpip_display_name, " +
    //       ":fpip_lan_id, " +
    //       ":fpip_do_json); END;"



    m_timer.start ();

    m_stmt_put_float_pvt_ips_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_float_pvt_ips_rg.setObject (2, ls_id);
    m_stmt_put_float_pvt_ips_rg.setObject (3, ls_fencing_token);

    m_stmt_put_float_pvt_ips_rg.setObject (4, fpip_id);
    m_stmt_put_float_pvt_ips_rg.setObject (5, fpip_subn_id);

    m_stmt_put_float_pvt_ips_rg.setObject (6, fpip_ip_address_int);
    m_stmt_put_float_pvt_ips_rg.setObject (7, fpip_time_created); 
    
    m_stmt_put_float_pvt_ips_rg.setObject (8, fpip_display_name);
    m_stmt_put_float_pvt_ips_rg.setObject (9, fpip_lan_id);

    m_stmt_put_float_pvt_ips_rg.setObject (10, fpip_do_json);


    m_stmt_put_float_pvt_ips_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_float_pvt_ips_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_float_pvt_ips_rg.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_float_pvt_ips_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_float_pvt_ips_rg", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_float_pvt_ips_rg updated_rows: " + 
      updated_rows); 



    return;
  }


  // ----------------------------------------
  // del_float_pvt_ip_atts_rg ()
  private void del_float_pvt_ip_atts_rg () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;

    Object fpipatt_id = "0";


    
    m_manager.log (m_threadName, "Preparing del_float_pvt_ip_atts_rg input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random ID from FLOAT_PVT_IP_ATTS_RG_DATA
    fpipatt_id = m_manager.getCachedValue (m_threadName, "FLOAT_PVT_IP_ATTS_RG_DATA", "ID");



    m_manager.log (m_threadName, "Calling del_float_pvt_ip_atts_rg ...");

    // Prepare to call PLSQL:  
    // "BEGIN ; del_float_pvt_ip_atts_rg (" +
    //             "        :lease_valid, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :fpipatt_id); END;"

    m_timer.start ();

    m_stmt_del_float_pvt_ip_atts_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_float_pvt_ip_atts_rg.setObject (2, ls_id);
    m_stmt_del_float_pvt_ip_atts_rg.setObject (3, ls_fencing_token);
    m_stmt_del_float_pvt_ip_atts_rg.setObject (4, fpipatt_id);

    m_stmt_del_float_pvt_ip_atts_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_float_pvt_ip_atts_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_float_pvt_ip_atts_rg.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_float_pvt_ip_atts_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_float_pvt_ip_atts_rg", 
      m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_float_pvt_ip_atts_rg lease valid: " + 
      lease_valid); 

    return;
  }


  // ----------------------------------------
  // put_float_pvt_ip_atts_rg ()
  private void put_float_pvt_ip_atts_rg () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN vcn.put_float_pvt_ip_atts_rg (" + 
    //             "        :updated_rows, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :fpipatt_id, " +
    //             "        :fpipatt_fpip_id, " +
    //             "        :fpipatt_nic_id, " +
    //             "        :fpipatt_nat_ip_addr, " +
    //             "        :fpiatt_lifecycle_state, " +
    //             "        :fpipatt_do_json); END;"

    Object fpipatt_id = "0";
    Object fpipatt_fpip_id = "0";
    Object fpipatt_nic_id = "0";
    Object fpipatt_nat_ip_addr = "0";
    Object fpiatt_lifecycle_state = "0";
    Object fpipatt_do_json = "0";


    m_manager.log (m_threadName, "Preparing put_float_pvt_ip_atts_rg input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from FLOAT_PVT_IP_ATTS_RG_DATA
    row = m_manager.getCachedRow (m_threadName, "FLOAT_PVT_IP_ATTS_RG_DATA");

    fpipatt_id = row.get ("ID"); 
    fpipatt_fpip_id = row.get ("FLOAT_PRIVATE_IP_ID");
    fpipatt_nic_id = row.get ("NIC_ID");
    fpipatt_nat_ip_addr = row.get ("NAT_IP_ADDRESS");
    fpiatt_lifecycle_state = row.get ("LIFECYCLE_STATE"); 
    fpipatt_do_json = row.get ("DO_JSON");   
    





    m_manager.log (m_threadName, "Calling put_float_pvt_ip_atts_rg ...");

    // "BEGIN vcn.put_float_pvt_ip_atts_rg (" + 
    //             "        :updated_rows, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :fpipatt_id, " +
    //             "        :fpipatt_fpip_id, " +
    //             "        :fpipatt_nic_id, " +
    //             "        :fpipatt_nat_ip_addr, " +
    //             "        :fpiatt_lifecycle_state, " +
    //             "        :fpipatt_do_json); END;"



    m_timer.start ();

    m_stmt_put_float_pvt_ip_atts_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_float_pvt_ip_atts_rg.setObject (2, ls_id);
    m_stmt_put_float_pvt_ip_atts_rg.setObject (3, ls_fencing_token);

    m_stmt_put_float_pvt_ip_atts_rg.setObject (4, fpipatt_id);
    m_stmt_put_float_pvt_ip_atts_rg.setObject (5, fpipatt_fpip_id);

    m_stmt_put_float_pvt_ip_atts_rg.setObject (6, fpipatt_nic_id);
    m_stmt_put_float_pvt_ip_atts_rg.setObject (7, fpipatt_nat_ip_addr);

    m_stmt_put_float_pvt_ip_atts_rg.setObject (8, fpiatt_lifecycle_state);
    m_stmt_put_float_pvt_ip_atts_rg.setObject (9, fpipatt_do_json);


    m_stmt_put_float_pvt_ip_atts_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_float_pvt_ip_atts_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_float_pvt_ip_atts_rg.getInt (1);

    // done
    m_conn.commit ();



    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_float_pvt_ip_atts_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_float_pvt_ip_atts_rg", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_float_pvt_ip_atts_rg updated_rows: " + 
      updated_rows); 



    return;
  }

 
  // ----------------------------------------
  // del_float_ips_rg ()
  private void del_float_ips_rg () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;

    // "BEGIN del_float_ips_rg " +
    // "        :lease_valid, " +
    // "        :ls_id,  " +
    // "        :ls_fencing_token,  " +
    // "        :fip_id); END;"

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object fip_id = "0";

    
    m_manager.log (m_threadName, "Preparing del_float_ips_rg input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random row from FLOAT_IPS_RG_DATA
    fip_id = m_manager.getCachedValue (m_threadName, "FLOAT_IPS_RG_DATA", "ID");

   



    m_manager.log (m_threadName, "Calling del_float_ips_rg ...");

    // "BEGIN del_float_ips_rg " +
    // "        :lease_valid, " +
    // "        :ls_id,  " +
    // "        :ls_fencing_token,  " +
    // "        :fip_id); END;"

    m_timer.start ();

    m_stmt_del_float_ips_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_float_ips_rg.setObject (2, ls_id);
    m_stmt_del_float_ips_rg.setObject (3, ls_fencing_token);
    m_stmt_del_float_ips_rg.setObject (4, fip_id);

    m_stmt_del_float_ips_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_float_ips_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_float_ips_rg.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_float_ips_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_float_ips_rg", m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_float_ips_rg lease valid: " + lease_valid); 

    return;
  }
  

  // ----------------------------------------
  // put_float_ips_rg ()
  private void put_float_ips_rg () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;


    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN vcn.put_float_ips_rg ( " +
    //       " :updated_rows " +
    //       " :ls_id " +
    //       " :ls_fencing_token " +
    //       " :fip_id " +
    //       " :fip_ip_addr " +
    //       " :fip_comp_id " +
    //       " :fip_scope " +
    //       " :fip_availability_domain " +
    //       " :fip_lifetime " +
    //       " :fip_public_ip_pool_id " +
    //       " :fip_do_json); END;"

    Object fip_id = "0";
    Object fip_ip_addr = "0";
    Object fip_comp_id = "0";
    Object fip_scope = "0";
    Object fip_availability_domain = "0";
    Object fip_lifetime = "0";
    Object fip_public_ip_pool_id = "0";
    Object fip_do_json = "0";


    m_manager.log (m_threadName, "Preparing put_float_ips_rg input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from FLOAT_IPS_RG_DATA
    row = m_manager.getCachedRow (m_threadName, "FLOAT_IPS_RG_DATA");

    fip_id = row.get ("ID"); 
    fip_ip_addr = row.get ("IP_ADDRESS");
    fip_comp_id = row.get ("COMPARTMENT_ID");
    fip_scope = row.get ("SCOPE");
    fip_availability_domain = row.get ("AVAILABILITY_DOMAIN"); 
    fip_lifetime = row.get ("LIFETIME");
    fip_public_ip_pool_id = row.get ("PUBLIC_IP_POOL_ID");
    fip_do_json = row.get ("DO_JSON");



    m_manager.log (m_threadName, "Calling put_float_ips_rg ...");

    // "BEGIN vcn.put_float_ips_rg ( " +
    //       " :updated_rows " +
    //       " :ls_id " +
    //       " :ls_fencing_token " +
    //       " :fip_id " +
    //       " :fip_ip_addr " +
    //       " :fip_comp_id " +
    //       " :fip_scope " +
    //       " :fip_availability_domain " +
    //       " :fip_lifetime " +
    //       " :fip_public_ip_pool_id " +
    //       " :fip_do_json); END;"



    m_timer.start ();

    m_stmt_put_float_ips_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_float_ips_rg.setObject (2, ls_id);
    m_stmt_put_float_ips_rg.setObject (3, ls_fencing_token);

    m_stmt_put_float_ips_rg.setObject (4, fip_id);
    m_stmt_put_float_ips_rg.setObject (5, fip_ip_addr);

    m_stmt_put_float_ips_rg.setObject (6, fip_comp_id);
    m_stmt_put_float_ips_rg.setObject (7, fip_scope);

    m_stmt_put_float_ips_rg.setObject (8, fip_availability_domain);
    m_stmt_put_float_ips_rg.setObject (9, fip_lifetime);

    m_stmt_put_float_ips_rg.setObject (10, fip_public_ip_pool_id);
    m_stmt_put_float_ips_rg.setObject (11, fip_do_json);


    m_stmt_put_float_ips_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_float_ips_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_float_ips_rg.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_float_ips_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_float_ips_rg", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_float_ips_rg updated_rows: " + 
      updated_rows); 



    return;
  }


  // ----------------------------------------
  // put_cache_status ()
  private void put_cache_status () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN ; vcn.put_cache_status (" +
    //             "        :updated_rows, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :cs_id, " +
    //             "        :cs_cursor, " +
    //             "        :cs_commit_txn_id, " +
    //             "        :cs_status, " +
    //             "        :cs_buckets) END;"

    Object cs_id = "0";
    Object cs_cursor = "0";
    Object cs_commit_txn_id = 0;
    Object cs_status = "0";
    Object cs_buckets = "0";


    m_manager.log (m_threadName, "Preparing put_cache_status input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from CACHE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "CACHE_STATUS_DATA");

    cs_id = row.get ("ID"); 
    cs_cursor = row.get ("CURSOR"); 
    cs_commit_txn_id = row.get ("COMMIT_TXN_ID"); 
    cs_status = row.get ("STATUS"); 
    cs_buckets = row.get ("BUCKETS"); 







    m_manager.log (m_threadName, "Calling put_cache_status ...");

    // "BEGIN ; vcn.put_cache_status (" +
    //             "        :updated_rows, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :cs_id, " +
    //             "        :cs_cursor, " +
    //             "        :cs_commit_txn_id, " +
    //             "        :cs_status, " +
    //             "        :cs_buckets) END;"



    m_timer.start ();

    m_stmt_put_cache_status.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_cache_status.setObject (2, ls_id);
    m_stmt_put_cache_status.setObject (3, ls_fencing_token);

    m_stmt_put_cache_status.setObject (4, cs_id);
    m_stmt_put_cache_status.setObject (5, cs_cursor);

    m_stmt_put_cache_status.setObject (6, cs_commit_txn_id);
    m_stmt_put_cache_status.setObject (7, cs_status);

    m_stmt_put_cache_status.setObject (8, cs_buckets);

    m_stmt_put_cache_status.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_cache_status.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_cache_status.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_cache_status elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_cache_status", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_cache_status updated_rows: " + 
      updated_rows); 



    return;
  }


 
  // ----------------------------------------
  // del_float_ip_atts_rg ()
  private void del_float_ip_atts_rg () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;

    // "BEGIN vcn.del_float_ip_atts_rg (" +
    //             "        :lease_valid, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :fipatt_id); END;"

    Object lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object fipatt_id = "0";

    
    m_manager.log (m_threadName, "Preparing del_float_ip_atts_rg input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random id from FLOAT_IP_ATTS_RG_DATA
    fipatt_id = m_manager.getCachedValue(m_threadName, "FLOAT_IP_ATTS_RG_DATA", "ID");

   



    m_manager.log (m_threadName, "Calling del_float_ip_atts_rg ...");

    // "BEGIN vcn.del_float_ip_atts_rg (" +
    //             "        :lease_valid, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :fipatt_id); END;"

    m_timer.start ();

    m_stmt_del_float_ip_atts_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_float_ip_atts_rg.setObject (2, ls_id);
    m_stmt_del_float_ip_atts_rg.setObject (3, ls_fencing_token);
    m_stmt_del_float_ip_atts_rg.setObject (4, fipatt_id);

    m_stmt_del_float_ip_atts_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_float_ip_atts_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_float_ip_atts_rg.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_float_ip_atts_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_float_ip_atts_rg", m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_float_ip_atts_rg lease valid: " + lease_valid); 

    return;
  }
  

  // ----------------------------------------
  // put_float_ip_atts_rg ()
  private void put_float_ip_atts_rg () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN vcn.put_float_ip_atts_rg (" +
    //       "   updated_rows, " +
    //       "   ls_id, " +
    //       "   ls_fencing_token, " +
    //       "   fipatt_id, " +
    //       "   fipatt_nic_id, " +
    //       "   fipatt_float_ip_id, " +
    //       "   fipatt_float_private_ip_id, " +
    //       "   fipatt_assigned_entity_id, " +
    //       "   fipatt_lifecycle_state, " +
    //       "   fipatt_do_json); END;"

    Object fipatt_id = "0";
    Object fipatt_nic_id = "0";
    Object fipatt_float_ip_id = "0";
    Object fipatt_float_private_ip_id = "0";
    Object fipatt_assigned_entity_id = "0";
    Object fipatt_lifecycle_state = "0";
    Object fipatt_do_json = "0";


    m_manager.log (m_threadName, "Preparing put_float_ip_atts_rg input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from FLOAT_IP_ATTS_RG_DATA
    row = m_manager.getCachedRow (m_threadName, "FLOAT_IP_ATTS_RG_DATA");

    fipatt_id = row.get ("ID"); 
    fipatt_nic_id = row.get ("NIC_ID"); 
    fipatt_float_ip_id = row.get ("FLOAT_IP_ID"); 
    fipatt_lifecycle_state = row.get ("LIFECYCLE_STATE"); 
    fipatt_float_private_ip_id = row.get ("FLOAT_PRIVATE_IP_ID"); 
    fipatt_assigned_entity_id = row.get ("ASSIGNED_ENTITY_ID"); 
    fipatt_do_json = row.get ("DO_JSON"); 




    m_manager.log (m_threadName, "Calling put_float_ip_atts_rg ...");

    // "BEGIN vcn.put_float_ip_atts_rg (" +
    //       "   updated_rows, " +
    //       "   ls_id, " +
    //       "   ls_fencing_token, " +
    //       "   fipatt_id, " +
    //       "   fipatt_nic_id, " +
    //       "   fipatt_float_ip_id, " +
    //       "   fipatt_float_private_ip_id, " +
    //       "   fipatt_assigned_entity_id, " +
    //       "   fipatt_lifecycle_state, " +
    //       "   fipatt_do_json); END;"



    m_timer.start ();

    m_stmt_put_float_ip_atts_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_float_ip_atts_rg.setObject (2, ls_id);
    m_stmt_put_float_ip_atts_rg.setObject (3, ls_fencing_token);

    m_stmt_put_float_ip_atts_rg.setObject (4, fipatt_id);
    m_stmt_put_float_ip_atts_rg.setObject (5, fipatt_nic_id);

    m_stmt_put_float_ip_atts_rg.setObject (6, fipatt_float_ip_id);
    m_stmt_put_float_ip_atts_rg.setObject (7, fipatt_float_private_ip_id);

    m_stmt_put_float_ip_atts_rg.setObject (8, fipatt_assigned_entity_id);

    m_stmt_put_float_ip_atts_rg.setObject (9, fipatt_lifecycle_state);
    m_stmt_put_float_ip_atts_rg.setObject (10, fipatt_do_json);

    
    m_stmt_put_float_ip_atts_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_float_ip_atts_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_float_ip_atts_rg.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_float_ip_atts_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_float_ip_atts_rg", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_float_ip_atts_rg updated_rows: " + 
      updated_rows); 



    return;
  }


  // ----------------------------------------
  // del_subns_rg ()
  private void del_subns_rg () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;


    // "BEGIN vcn.del_subns_rg ( " +
    //       " :lease_valid, " +
    //       " :ls_id, " +
    //       " :ls_fencing_token, " +
    //       " :sn_id)    ; END;"

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object sn_id = "0";

    
    m_manager.log (m_threadName, "Preparing del_subns_rg input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random row from SUBNETS_RG_DATA
    sn_id = m_manager.getCachedValue(m_threadName, "SUBNETS_RG_DATA", "ID");


   



    m_manager.log (m_threadName, "Calling del_subns_rg ...");

    // "BEGIN vcn.del_subns_rg ( " +
    //       " :lease_valid, " +
    //       " :ls_id, " +
    //       " :ls_fencing_token, " +
    //       " :sn_id)    ; END;"

    m_timer.start ();

    m_stmt_del_subns_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_subns_rg.setObject (2, ls_id);
    m_stmt_del_subns_rg.setObject (3, ls_fencing_token);
    m_stmt_del_subns_rg.setObject (4, sn_id);

    m_stmt_del_subns_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_subns_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_subns_rg.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_subns_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_subns_rg", m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_subns_rg lease valid: " + lease_valid); 

    return;
  }
  

  // ----------------------------------------
  // put_subns_rg ()
  private void put_subns_rg () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN vcn.put_subns_rg (" +
    //           ":updated_rows, " +
    //           ":ls_id, " +
    //           ":ls_fencing_token, " +
    //           ":sn_id, " +
    //           ":sn_comp_id, " +
    //           ":sn_vcn_id, " +
    //           ":sn_rt_table_id, " +
    //           ":sn_dhcp_options_id, " +
    //           ":sn_dns_label, " +
    //           ":sn_display_name, " +
    //           ":sn_time_created, " +
    //           ":sn_do_json)   ; END;"

    Object sn_id = "0";
    Object sn_comp_id = "0";
    Object sn_vcn_id = "0";
    Object sn_rt_table_id = "0";
    Object sn_dhcp_options_id = "0";
    Object sn_dns_label = "0";
    Object sn_display_name = "0";
    Object sn_time_created = null;
    Object sn_do_json = "0";


    m_manager.log (m_threadName, "Preparing put_subns_rg input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from SUBNETS_RG_DATA
    row = m_manager.getCachedRow (m_threadName, "SUBNETS_RG_DATA");

    sn_id = row.get ("ID"); 
    sn_comp_id = row.get ("COMPARTMENT_ID"); 
    sn_vcn_id = row.get ("VCN_ID"); 
    sn_rt_table_id = row.get ("ROUTE_TABLE_ID"); 
    sn_dhcp_options_id = row.get ("DHCP_OPTIONS_ID"); 
    sn_dns_label = row.get ("DNS_LABEL"); 
    sn_display_name = row.get ("DISPLAY_NAME"); 
    sn_time_created = row.get ("TIME_CREATED"); 
    sn_do_json = row.get ("DO_JSON");  




    m_manager.log (m_threadName, "Calling put_subns_rg ...");

    // "BEGIN vcn.put_subns_rg (" +
    //           ":updated_rows, " +
    //           ":ls_id, " +
    //           ":ls_fencing_token, " +
    //           ":sn_id, " +
    //           ":sn_comp_id, " +
    //           ":sn_vcn_id, " +
    //           ":sn_rt_table_id, " +
    //           ":sn_dhcp_options_id, " +
    //           ":sn_dns_label, " +
    //           ":sn_display_name, " +
    //           ":sn_time_created, " +
    //           ":sn_do_json)   ; END;"



    m_timer.start ();

    m_stmt_put_subns_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_subns_rg.setObject (2, ls_id);
    m_stmt_put_subns_rg.setObject (3, ls_fencing_token);

    m_stmt_put_subns_rg.setObject (4, sn_id);
    m_stmt_put_subns_rg.setObject (5, sn_comp_id);

    m_stmt_put_subns_rg.setObject (6, sn_vcn_id);
    m_stmt_put_subns_rg.setObject (7, sn_rt_table_id);

    m_stmt_put_subns_rg.setObject (8, sn_dhcp_options_id);

    m_stmt_put_subns_rg.setObject (9, sn_dns_label);
    m_stmt_put_subns_rg.setObject (10, sn_display_name);

    m_stmt_put_subns_rg.setObject (11, sn_time_created);
    m_stmt_put_subns_rg.setObject (12, sn_do_json);


    
    m_stmt_put_subns_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_subns_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_subns_rg.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_subns_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_subns_rg", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_subns_rg updated_rows: " + 
      updated_rows); 



    return;
  }


  // ----------------------------------------
  // del_net_sec_group_rg ()
  private void del_net_sec_group_rg () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    // "BEGIN vcn.del_net_sec_group_rg (" +
    //                 "        :lease_valid, " +
    //                 "        :ls_id, " +
    //                 "        :ls_fencing_token, " +
    //                 "        :nsg_id); END;"

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object nsg_id = "0";

    
    m_manager.log (m_threadName, "Preparing del_net_sec_group_rg input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random row from NET_SEC_GROUP_RG_DAT
    nsg_id = m_manager.getCachedValue(m_threadName, "NET_SEC_GROUP_RG_DAT", "ID");

   



    m_manager.log (m_threadName, "Calling del_net_sec_group_rg ...");

    // "BEGIN vcn.del_net_sec_group_rg (" +
    //                 "        :lease_valid, " +
    //                 "        :ls_id, " +
    //                 "        :ls_fencing_token, " +
    //                 "        :nsg_id); END;"

    m_timer.start ();

    m_stmt_del_net_sec_group_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_net_sec_group_rg.setObject (2, ls_id);
    m_stmt_del_net_sec_group_rg.setObject (3, ls_fencing_token);
    m_stmt_del_net_sec_group_rg.setObject (4, nsg_id);

    m_stmt_del_net_sec_group_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_net_sec_group_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_net_sec_group_rg.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_net_sec_group_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_net_sec_group_rg", m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_net_sec_group_rg lease valid: " + lease_valid); 

    return;
  }
  
// ----------------------------------------
  // put_net_sec_group_rg ()
  private void put_net_sec_group_rg () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN ; vcn.put_net_sec_group_rg (" +
    //                 "        :updated_rows, " +
    //                 "        :ls_id, " +
    //                 "        :ls_fencing_token, " +
    //                 "        :nsg_id, " +
    //                 "        :nsg_dp_id, " +
    //                 "        :nsg_comp_id, " +
    //                 "        :nsg_vcn_id, " +
    //                 "        :nsg_display_name, " +
    //                 "        :nsg_time_created, " +
    //                 "        :nsg_do_json) END;"

    Object nsg_id = "0";
    Object nsg_dp_id = "0";
    Object nsg_comp_id = "0";
    Object nsg_vcn_id = "0";
    Object nsg_display_name = "0";
    Object nsg_time_created = null;
    Object nsg_do_json = "0";


    m_manager.log (m_threadName, "Preparing put_net_sec_group_rg input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from NET_SEC_GROUP_RG_DAT
    row = m_manager.getCachedRow (m_threadName, "NET_SEC_GROUP_RG_DAT");
  
    nsg_id = row.get ("ID"); 
    nsg_dp_id = row.get ("DP_ID"); 
    nsg_comp_id = row.get ("COMPARTMENT_ID"); 
    nsg_vcn_id = row.get ("VCN_ID"); 
    nsg_display_name = row.get ("DISPLAY_NAME"); 
    nsg_time_created = row.get ("TIME_CREATED"); 
    nsg_do_json = row.get ("DO_JSON"); 






    m_manager.log (m_threadName, "Calling put_net_sec_group_rg ...");

    // "BEGIN ; vcn.put_net_sec_group_rg (" +
    //                 "        :updated_rows, " +
    //                 "        :ls_id, " +
    //                 "        :ls_fencing_token, " +
    //                 "        :nsg_id, " +
    //                 "        :nsg_dp_id, " +
    //                 "        :nsg_comp_id, " +
    //                 "        :nsg_vcn_id, " +
    //                 "        :nsg_display_name, " +
    //                 "        :nsg_time_created, " +
    //                 "        :nsg_do_json) END;"



    m_timer.start ();

    m_stmt_put_net_sec_group_rg.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_net_sec_group_rg.setObject (2, ls_id);
    m_stmt_put_net_sec_group_rg.setObject (3, ls_fencing_token);

    m_stmt_put_net_sec_group_rg.setObject (4, nsg_id);
    m_stmt_put_net_sec_group_rg.setObject (5, nsg_dp_id);

    m_stmt_put_net_sec_group_rg.setObject (6, nsg_comp_id);
    m_stmt_put_net_sec_group_rg.setObject (7, nsg_vcn_id);

    m_stmt_put_net_sec_group_rg.setObject (8, nsg_display_name);

    m_stmt_put_net_sec_group_rg.setObject (9, nsg_time_created);
    m_stmt_put_net_sec_group_rg.setObject (10, nsg_do_json);


    
    m_stmt_put_net_sec_group_rg.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_net_sec_group_rg.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_net_sec_group_rg.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_net_sec_group_rg elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_net_sec_group_rg", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_net_sec_group_rg updated_rows: " + 
      updated_rows); 



    return;
  }

  // ----------------------------------------
  // query_get_nic ()
  private void query_get_nic () throws SQLException
  {
    // locals
    ResultSet rs;

    // "SELECT do_json FROM NICS_RG WHERE id = ?"

    Object nic_id = "0";
    String nic_do_json = "0";


    m_manager.log (m_threadName, "Preparing query_get_nic input ...");
    
    // select a random id from NICS_RG_DATA
    nic_id = m_manager.getCachedValue (m_threadName, "NICS_RG_DATA", "ID");


    m_manager.log (m_threadName, "Calling query_get_nic ...");

    m_timer.start ();

    m_stmt_query_get_nic.setObject (1, nic_id);
    rs = m_stmt_query_get_nic.executeQuery();

    while (rs.next ())
    {
      nic_do_json = rs.getString (1);
    }

    rs.close ();

    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "query_get_nic elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("query_get_nic", m_timer.getTimeInUs ());



    // output
    m_manager.log (m_threadName, "query_get_nic do_json: " + nic_do_json); 

    return;
  }



  // ----------------------------------------
  // query_get_net_sec_groups ()
  private void query_get_net_sec_groups () throws SQLException
  {
    // locals
    ResultSet rs;


    // "SELECT NSG.do_json FROM NET_SEC_GROUP_RG NSG " + 
    // "INNER JOIN NSG_ASSOC_RG NSG_ASC " + 
    // "ON NSG.id = NSG_ASC.nsg_id " + 
    // "WHERE NSG_ASC.nic_id = ?"

    Object nic_id = "0";
    String nsg_do_json = "0";


    m_manager.log (m_threadName, "Preparing query_get_net_sec_groups input ...");
  
    // select a random id from the table
    nic_id = m_manager.getCachedValue (m_threadName, "NSG_ASSOC_RG_DATA", "NIC_ID");

    

    m_manager.log (m_threadName, "Calling query_get_net_sec_groups ...");

    m_timer.start ();

    m_stmt_query_get_net_sec_groups.setObject (1, nic_id);
    rs = m_stmt_query_get_net_sec_groups.executeQuery();

    while (rs.next ())
    {
      nsg_do_json = rs.getString (1);
    }

    rs.close ();

    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "query_get_net_sec_groups elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("query_get_net_sec_groups", m_timer.getTimeInUs ());



    // output
    m_manager.log (m_threadName, "query_get_net_sec_groups do_json: " + nsg_do_json); 

    return;
  }


  // ----------------------------------------
  // query_get_prim_pub_ip ()
  private void query_get_prim_pub_ip () throws SQLException
  {
    // locals
    ResultSet rs;


    // "SELECT FIPs.do_json, FIPAs.do_json FROM FLOAT_IP_ATTS_RG FIPAs " +
    // "INNER JOIN FLOAT_IPS_RG FIPs " +
    // "ON FIPAs.float_ip_id = FIPs.id " +
    // "WHERE FIPAs.float_private_ip_id = ? " +
    // "AND FIPAs.lifecycle_state IN ('AVAILABLE', 'PROVISIONING')"

    Object float_private_ip_id = "0";
    String fips_do_json = "0";
    String fipas_do_json = "0";

    m_manager.log (m_threadName, "Preparing query_get_prim_pub_ip input ...");
  
    // select random IDs from the table
    float_private_ip_id = m_manager.getCachedValue (m_threadName, "FLOAT_IP_ATTS_RG_DATA", "FLOAT_PRIVATE_IP_ID");


    m_manager.log (m_threadName, "Calling query_get_prim_pub_ip ...");

    m_timer.start ();

    m_stmt_query_get_prim_pub_ip.setObject (1, float_private_ip_id);
    rs = m_stmt_query_get_prim_pub_ip.executeQuery();

    while (rs.next ())
    {
      fips_do_json = rs.getString (1);
      fipas_do_json = rs.getString (2);
    }

    rs.close ();

    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "query_get_prim_pub_ip elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("query_get_prim_pub_ip", m_timer.getTimeInUs ());



    // output
    m_manager.log (m_threadName, "query_get_prim_pub_ip fips_do_json: " + fips_do_json); 
    m_manager.log (m_threadName, "query_get_prim_pub_ip fipas_do_json: " + fipas_do_json); 

    return;
  }


  // ----------------------------------------
  // query_get_prim_priv_ip ()
  private void query_get_prim_priv_ip () throws SQLException
  {
    // locals
    ResultSet rs;


    // "SELECT FPIPs.do_json, FPIPAs.do_json FROM FLOAT_PVT_IP_ATTS_RG FPIPAs " +
    // "INNER JOIN FLOAT_PVT_IPS_RG FPIPs " +
    // "ON FPIPAs.float_private_ip_id = FPIPs.id " +
    // "WHERE FPIPAs.nic_id = ? " +
    // "AND FPIPAs.lifecycle_state IN ('AVAILABLE', 'PROVISIONING')"

    Object nic_id = "0";
    String fips_do_json = "0";
    String fipas_do_json = "0";

    m_manager.log (m_threadName, "Preparing query_get_prim_priv_ip input ...");
  
    // select random IDs from the table
    nic_id = m_manager.getCachedValue (m_threadName, "FLOAT_PVT_IP_ATTS_RG_DATA", "NIC_ID");




    m_manager.log (m_threadName, "Calling query_get_prim_priv_ip ...");

    m_timer.start ();

    m_stmt_query_get_prim_priv_ip.setObject (1, nic_id); 
    rs = m_stmt_query_get_prim_priv_ip.executeQuery();

    while (rs.next ())
    {
      fips_do_json = rs.getString (1);
      fipas_do_json = rs.getString (2);
    }

    rs.close ();

    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "query_get_prim_priv_ip elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("query_get_prim_priv_ip", m_timer.getTimeInUs ());



    // output
    m_manager.log (m_threadName, "query_get_prim_priv_ip fips_do_json: " + fips_do_json); 
    m_manager.log (m_threadName, "query_get_prim_priv_ip fipas_do_json: " + fipas_do_json); 

    return;
  }


  // ----------------------------------------
  // query_get_ip_addr_nic ()
  private void query_get_ip_addr_nic () throws SQLException
  {
    // locals
    ResultSet rs;


    // "SELECT do_json, time_created FROM VCN.IP_RG " +
    // "WHERE nic_id = ? " +
    // "AND rownum <= 32 " +
    // "ORDER BY time_created ASC"

    Object nic_id = "0";
    String do_json = "0";
    java.sql.Timestamp time_created = null;

    m_manager.log (m_threadName, "Preparing query_get_ip_addr_nic input ...");
  
    // select random IDs from the table
    nic_id = m_manager.getCachedValue (m_threadName, "IP_RG_DATA", "NIC_ID");



    m_manager.log (m_threadName, "Calling query_get_ip_addr_nic ...");

    m_timer.start ();

    m_stmt_query_get_ip_addr_nic.setObject (1, nic_id); 
    rs = m_stmt_query_get_ip_addr_nic.executeQuery();

    while (rs.next ())
    {
      do_json = rs.getString (1);
      time_created = rs.getTimestamp (2);
    }

    rs.close ();

    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "query_get_ip_addr_nic elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("query_get_ip_addr_nic", m_timer.getTimeInUs ());



    // output
    m_manager.log (m_threadName, "query_get_ip_addr_nic do_json: " + do_json); 
    m_manager.log (m_threadName, "query_get_ip_addr_nic time_created: " + time_created); 

    return;
  }






  // ----------------------------------------
  // getLeaseStatusData ()
  // Returns a random ID and FENCING_TOKEN from the LEASE_STATUS_DATA
  // table. The first element in the returned array list is ID (String) followed 
  // by FENCING_TOKEN (int).
  private ArrayList<Object> getLeaseStatusData () throws SQLException
  {

    PreparedStatement pstmt = null;
    ResultSet rsRow = null;
    String sql, sqlRows;

    ArrayList<Object> leaseData = new ArrayList<>();


    // select a random row from LEASE_STATUS_DATA
    sqlRows = getRowSelectionClause ("LEASE_STATUS_DATA");
    sql = "SELECT " + sqlRows + " ID, FENCING_TOKEN FROM LEASE_STATUS_DATA";
    m_manager.log (m_threadName, sql);

    pstmt = m_conn.prepareStatement (sql);
    rsRow = pstmt.executeQuery();

    if (rsRow.next ())
    {
      leaseData.add (rsRow.getString (1));
      leaseData.add (rsRow.getInt (2));
    }

    pstmt.close ();

    return leaseData;
  }


  // ----------------------------------------
  // getTableIdData ()
  // 
  private String getTableIdData (String tableName) throws SQLException
  {
    PreparedStatement pstmt;
    ResultSet rsRow;

    String id = "0";
    String sql, sqlRows;

    // select a random ID from the table
    sqlRows = getRowSelectionClause (tableName);
    sql = "SELECT " + sqlRows + " ID FROM " + tableName;
    m_manager.log (m_threadName, sql);

    pstmt = m_conn.prepareStatement (sql);
    rsRow = pstmt.executeQuery();

    if (rsRow.next ())
    {
      id = rsRow.getString (1);
    }

    pstmt.close ();
   
    return id;
  }

  // ----------------------------------------
  // getRowSelectionClause ()
  // Returns a random "ROWS N TO M" clause based on the table row count
  private String getRowSelectionClause (String tableName)
  {
    String rowSelectionClause = null;
    int rowCount = 0;
    int row = 0;

    rowCount = m_manager.getTableRowCount (tableName);

    if (rowCount != 0)
      row = m_rand.nextInt (rowCount) + 1;

    rowSelectionClause = "ROWS " + row + " TO " + row;


    return rowSelectionClause;
  }
 

  
  // ----------------------------------------
  // getStatus ()
  // Returns the test status of this thread
  public boolean getStatus ()
  {
    return m_status;
  }


  // ----------------------------------------
  // getReady ()
  // Returns true if the thread is ready to run
  public boolean getReady ()
  {
    return m_ready;
  }  

  // ----------------------------------------
  // setStart ()
  // Indicate that the thread ready to start running
  public void setStart ()
  {
    m_start = true;
    return;
  }

  private int getRandomIndex(double[] probabilities, Random random) 
  {
    double r = random.nextDouble();
    double cumulativeProbability = 0;

    for (int i = 0; i < probabilities.length; i++) {

        cumulativeProbability += probabilities[i];

        if (r <= cumulativeProbability) {
            return i;
        }
    }

    return probabilities.length - 1;
  }



}


// Measure in microseconds
class TTTimer {

  public static String curTime() {
      DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss");
      LocalTime localTime = LocalTime.now();
      return dtf.format(localTime);    // 16:37:15
  }
  
  private long startTime = -1;
  private long endTime = -1;
  
  public long start() {

      startTime = System.nanoTime() / 1000;

      return startTime;
  }

  public long stop() {
      
      endTime = System.nanoTime() / 1000;

      return endTime;
  }

  public long getCurrent() {
      return System.nanoTime() / 1000;
  }

  public long getElapsed() {
      if ( startTime <= 0  )
          return 0;
      else
          return ((System.nanoTime() / 1000) - startTime);

  }

  public long getTimeInUs() {

    if((startTime == -1) || (endTime == -1)) {
        return -1;
    }
    else if((endTime == startTime)) {
        return 1;
    }
    else
        return (endTime - startTime);

  }
}


