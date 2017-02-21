using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace eXpatsScheduler
{
    class SQLHelper
    {
        public static SqlConnection GetConnection()
        {
            SqlConnection output = new SqlConnection(GetConnectionString());
            output.Open();
            return output;
        }

        public static string GetConnectionString()
        {
            string output = "Data Source=10.20.1.8;Initial Catalog=MOF;User ID=appanalyst;Password=Mdec@cyber";
            return output;
        }

        public static SqlConnection GetConnectionCRM()
        {
            SqlConnection output = new SqlConnection(GetConnectionStringCRM());
            output.Open();
            return output;
        }

        public static string GetConnectionStringCRM()
        {
            string output = "Data Source=10.9.113.28;Initial Catalog=CRM_PRD;User ID=appadmin;Password=Mdec@cyber1215";
            return output;
        }
    }
}
