using System;
using System.Configuration;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.IO;

namespace LMSTimeSheet
{
    static class Program
    {
        static void Main()
        {

            try
            {

                // Connection string 
                var lmsConnection = ConfigurationManager.ConnectionStrings["LMSConnection"].ConnectionString;
                string msAccessConnection = ConfigurationManager.ConnectionStrings["MSAccessConnection"].ConnectionString;
                string FetchDateFrom = ConfigurationManager.AppSettings["FetchDateFrom"];
                WriteLogFile(DateTime.Now + ": Started Processing Timesheet data");
                Console.WriteLine(DateTime.Now + ": Started Processing Timesheet data");
                //Get lastUpdated time for processing 

                string lastUpdatedDateTime = CheckTimeSheetLastTime(lmsConnection);
                if (string.IsNullOrEmpty(lastUpdatedDateTime))
                {
                    lastUpdatedDateTime = FetchDateFrom;
                }
                //Get the access Details

                var accessResults = GetMsAccessDetails(msAccessConnection, lastUpdatedDateTime);

                //Adding new TimeSheet Details

                if (accessResults.Rows.Count > 0)
                {
                    WriteLogFile(DateTime.Now + ": Adding MSAccess " + accessResults.Rows.Count + " rows To LMS DataBase");
                    Console.WriteLine(DateTime.Now + ": Adding  MSAccess " + accessResults.Rows.Count + " rows To LMS DataBase");

                    BulkInsertToTimeSheet(lmsConnection, accessResults);
                    // reset
                    WriteLogFile(DateTime.Now + ": Processed MSAccess " + accessResults.Rows.Count + " rows To LMS DataBase");
                    Console.WriteLine(DateTime.Now + ": Adding  MSAccess " + accessResults.Rows.Count + " rows To LMS DataBase");
                }
                else
                {
                    WriteLogFile(DateTime.Now + ": No MsAccessRecord for Processing Timesheet");
                    Console.WriteLine(DateTime.Now + ": No MsAccessRecord for Processing Timesheet");
                }
                WriteLogFile(DateTime.Now + ": Mapping Employee with Timesheet");
                Console.WriteLine(DateTime.Now + ": Mapping Employee with Timesheet");

                MapEmployeeWithTimesheet(lmsConnection);

                WriteLogFile(DateTime.Now + ": Processed Timesheet " + accessResults.Rows.Count + " rows");
                Console.WriteLine(DateTime.Now + ": Processed Timesheet " + accessResults.Rows.Count + " rows");
                accessResults.Clear();


            }
            catch (Exception e)
            {

                Console.WriteLine(DateTime.Now + ": Process failed due to " + e.Message);

            }
        }

        private static string CheckTimeSheetLastTime(string lmsConnection)
        {

            SqlConnection conn = new SqlConnection(lmsConnection);
            conn.Open();
            SqlCommand comm = new SqlCommand("SELECT  MAX(INOUTDATE) FROM ACCESSTRANSACTIONS ", conn);
            var lastUpdatedTime = comm.ExecuteScalar();
            conn.Close();
            conn.Dispose();
            return lastUpdatedTime.ToString();
        }
        private static void MapEmployeeWithTimesheet(string lmsConnection)
        {
            using (SqlConnection con = new SqlConnection(lmsConnection))
            {
                using (SqlCommand cmd = new SqlCommand("Sp_MapEmployee_Timesheet", con))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
                con.Close();
                con.Dispose();
            }
        }


        private static DataTable GetMsAccessDetails(string msAccessConnection, string lastUpdatedDateTime)
        {
            WriteLogFile(DateTime.Now + ": Processing MsAccessDetails");
            Console.WriteLine(DateTime.Now + ": Processing MsAccessDetails");
            DataTable accessResults = new DataTable();
            using (OleDbConnection conn = new OleDbConnection(msAccessConnection))
            {
                var strSqLquery = "SELECT  Trans.Tid,Trans.CARDID, Trans.Dt, CBool(Trans.InOut)  FROM Trans";


                if (!string.IsNullOrEmpty(lastUpdatedDateTime))
                {
                    WriteLogFile(DateTime.Now + ": Processing " + lastUpdatedDateTime + " onwards data from MSAccess DataBase");
                    Console.WriteLine(DateTime.Now + ": Processing " + lastUpdatedDateTime + " onwards data from MSAccess DataBase");
                    strSqLquery =
                        "SELECT  Trans.Tid,Trans.CARDID, Trans.Dt,  CBool(Trans.InOut)   FROM Trans WHERE Trans.Dt >#" +
                        lastUpdatedDateTime + "#";
                }
                else
                {
                    WriteLogFile(DateTime.Now + ": Processing full data from MSAccess DataBase");
                    Console.WriteLine(DateTime.Now + ": Processing full data from MSAccess DataBase");
                }
                OleDbCommand cmd = new OleDbCommand(strSqLquery, conn);

                conn.Open();

                OleDbDataAdapter adapter = new OleDbDataAdapter(cmd);

                adapter.Fill(accessResults);
            }
            return accessResults;
        }

        private static void BulkInsertToTimeSheet(string lmsConnection, DataTable accessResults)
        {
            using (SqlConnection connection =
                new SqlConnection(lmsConnection))
            {
                // make sure to enable triggers
                // more on triggers in next post
                SqlBulkCopy bulkCopy =
                    new SqlBulkCopy
                        (
                            connection,
                            SqlBulkCopyOptions.TableLock |
                            SqlBulkCopyOptions.FireTriggers |
                            SqlBulkCopyOptions.UseInternalTransaction,
                            null
                        )
                    { DestinationTableName = "ACCESSTRANSACTIONS" };

                // set the destination table name
                connection.Open();

                // write the data in the "dataTable"
                bulkCopy.WriteToServer(accessResults);
                connection.Close();
            }
        }

        private static void WriteLogFile(string formattedMsg)
        {
            try
            {
                string path = AppDomain.CurrentDomain.BaseDirectory + "\\LMSTimeSheet.log";
                if (Directory.Exists(Path.GetDirectoryName(path)))
                {
                    File.AppendAllText(path, formattedMsg + Environment.NewLine);
                }
                else
                    Console.WriteLine("Failed to write to log file file for the path  " + formattedMsg + ". ");
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }
    }
}


