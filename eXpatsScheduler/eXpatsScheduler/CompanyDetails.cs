using System;
using System.Collections;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace eXpatsScheduler
{
    internal class CompanyDetails
    {
        internal static void Start()
        {
            ArrayList dtList = new ArrayList();
            //MaklumatSyarikat
            //DataTable dtComp = getMaklumatSyarikat();
            //DataTable dtComp = getMaklumatSyarikat1();
            //dtList.Add(dtComp);
            //MaklumatPremis
            //DataTable dtPremis = getMaklumatPremis();
            //dtList.Add(dtPremis);
            ////StrukturHakmilikSyarikat
            //DataTable dtHakMilik = getMaklumatHakMilik();
            //dtList.Add(dtHakMilik);
            //DataTable dtMaklumatPermohonan = getMaklumatPermohonan();
            //dtList.Add(dtMaklumatPermohonan);
            DataTable dtBA= getBA();
            dtList.Add(dtBA);
            StoreIntoMOF(dtList);
        }

        private static DataTable getBA()
        {
            using (SqlConnection conn = SQLHelper.GetConnectionCRM())
            {
                using (SqlCommand cmd = new SqlCommand("", conn))
                {
                    using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
                    {
                        StringBuilder sql = new StringBuilder();
                        sql.AppendLine("SELECT [RefNumber],[name] ,[AccountType] FROM[CRM_PRD].[dbo].[MOF_BA]");
                        cmd.CommandText = sql.ToString();
                        DataTable dt = new DataTable("MOF_BA");
                        adapter.Fill(dt);
                        return dt;
                    }
                }
            }
        }

        private static DataTable getMaklumatSyarikat1()
        {
            using (SqlConnection conn = SQLHelper.GetConnectionCRM())
            {
                using (SqlCommand cmd = new SqlCommand("", conn))
                {
                    using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
                    {
                        StringBuilder sql = new StringBuilder();
                        sql.AppendLine("SELECT[Company Name] AS CompanyName");
                        sql.AppendLine(",[File ID] AS MSCFileID");
                        sql.AppendLine(",[CoreActivities]");
                        sql.AppendLine(",[Registration Number] AS ROCNumber");
                        sql.AppendLine(",[Website URL] as URL");
                        sql.AppendLine(",[AC Meeting Date] ACMeetingDate");
                        sql.AppendLine(",[Approval Letter Date] ApprovalLetterDate");
                        sql.AppendLine(",[Year Of Approval] YearOfApproval");
                        sql.AppendLine(",[Date of Incorporation] as DateofIncorporation");
                        sql.AppendLine(",[Operational Status] as OperationalStatus");
                        sql.AppendLine(",[Financial Incentive] FinancialIncentive");
                        sql.AppendLine(",[Main Cluster] as Cluster");
                        sql.AppendLine(",[BusinessPhone]");
                        sql.AppendLine(",[Fax]");
                        sql.AppendLine(",[TaxRevenueLoss]");
                        sql.AppendLine(",[Tier]");
                        sql.AppendLine(",[SubmitDate]");
                        sql.AppendLine(",[MSCCertNo]");
                        sql.AppendLine(",[BA]");
                        sql.AppendLine(" FROM[CRM_PRD].[dbo].[MOF_MSC_COMPANY_31012017]");
                        cmd.CommandText = sql.ToString();
                        DataTable dt = new DataTable("MOF_MaklumatSyarikat1");
                        adapter.Fill(dt);
                        return dt;
                    }
                }
            }
        }

        private static void StoreIntoMOF(ArrayList dtList)
        {
            foreach (DataTable dt in dtList)
            {
                SaveIntoDB(dt, dt.TableName.ToString());
            }
        }

        private static void SaveIntoDB(DataTable dt, string TableName)
        {
            string strconnection = "Data Source = 10.20.1.8; Initial Catalog = MOF; User ID = appanalyst; Password = Mdec@cyber";
            string table = "";
            table += "IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[" + TableName + "]') AND type in (N'U'))";
            table += "BEGIN ";
            table += "create table " + TableName + "";
            table += "(";
            for (int i = 0; i < dt.Columns.Count; i++)
            {
                if (i != dt.Columns.Count - 1)
                    table += dt.Columns[i].ColumnName + " " + "varchar(max)" + ",";
                else
                    table += dt.Columns[i].ColumnName + " " + "varchar(max)";
            }
            table += ") ";
            table += "END";
            InsertQuery(table, strconnection);
            CopyData(strconnection, dt, TableName);
        }
        private static void InsertQuery(string qry, string connection)
        {


            SqlConnection _connection = new SqlConnection(connection);
            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = qry;
            cmd.Connection = _connection;
            _connection.Open();
            cmd.ExecuteNonQuery();
            _connection.Close();
        }
        public static void CopyData(string connStr, DataTable dt, string tablename)
        {
            using (SqlBulkCopy bulkCopy =
            new SqlBulkCopy(connStr, SqlBulkCopyOptions.TableLock))
            {
                bulkCopy.DestinationTableName = tablename;
                bulkCopy.WriteToServer(dt);
            }
        }
        private static DataTable getMaklumatHakMilik()
        {
            using (SqlConnection conn = SQLHelper.GetConnectionCRM())
            {
                using (SqlCommand cmd = new SqlCommand("", conn))
                {
                    using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
                    {
                        StringBuilder sql = new StringBuilder();
                        sql.AppendLine("select mscfileid,regionname, shareholdername, percentage");
                        sql.AppendLine("from dbo.BI_MSC_Shareholder where status = 1 ");
                        sql.AppendLine("and mscfileid iN(select[File ID] from[CRM_PRD].[dbo].[BI_MSC_Company] where[operational status] = 'Active')");
                        cmd.CommandText = sql.ToString();
                        DataTable dt = new DataTable("MOF_HakmilikSyarikat");
                        adapter.Fill(dt);
                        return dt;
                    }
                }
            }
        }

        private static DataTable getMaklumatPremis()
        {
            using (SqlConnection conn = SQLHelper.GetConnectionCRM())
            {
                using (SqlCommand cmd = new SqlCommand("", conn))
                {
                    using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
                    {
                        StringBuilder sql = new StringBuilder();
                        sql.AppendLine("select MScfileid, Address1, Address2,State,Postcode,City,BusinessPhone,Fax,");
                        sql.AppendLine("Country from dbo.BI_MSC_Address where master = 'Yes' and mscfileid in ");
                        sql.AppendLine("(select[File ID] from[CRM_PRD].[dbo].[BI_MSC_Company] where[operational status] = 'Active')");
                        cmd.CommandText = sql.ToString();
                        DataTable dt = new DataTable("MOF_MaklumatPremis");
                        adapter.Fill(dt);
                        return dt;
                    }
                }
            }
        }

        private static DataTable getMaklumatSyarikat()
        {
            using (SqlConnection conn = SQLHelper.GetConnectionCRM())
            {
                using (SqlCommand cmd = new SqlCommand("", conn))
                {
                    using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
                    {
                        StringBuilder sql = new StringBuilder();
                        sql.AppendLine("SELECT a.[Company Name] as CompanyName");
                        sql.AppendLine(", a.[File ID] as msc_file_id");
                        sql.AppendLine(", a.[Registration Number] as ROCNumber");
                        sql.AppendLine(", a.[Approval Date] AS [ACMeetingDate]");
                        sql.AppendLine(", a.[Approval Letter Date] as ApprovalLetterDate");
                        sql.AppendLine(", a.[Date of Incorporation] as DateofIncorporation");
                        sql.AppendLine(", a.[Operational Status] as [OperationalStatus]");
                        sql.AppendLine(", a.[Financial Incentive] as [FinancialIncentive]");
                        sql.AppendLine(" , a.[Main Cluster] as [MainCluster]");
                        sql.AppendLine(" , a.[CoreActivities]");
                        sql.AppendLine(" , a.[BusinessPhone]");
                        sql.AppendLine(", a.[Fax]");
                        sql.AppendLine(", a.TaxRevenueLoss");
                        sql.AppendLine(", a.tier as Tier");
                        sql.AppendLine(", b.EEManagerName");
                        sql.AppendLine(" , a.SubmitDate");
                        sql.AppendLine("FROM[CRM_PRD].[dbo].[BI_MSC_Company] a");
                        sql.AppendLine("LEFT JOIN dbo.BI_MSC_Account_Manager b ON b.mscfileid = a.[File ID]");
                        sql.AppendLine("where b.active = 'Yes' AND a.[operational status] = 'Active'");
                        cmd.CommandText = sql.ToString();
                        DataTable dt = new DataTable("MOF_MaklumatSyarikat");
                        adapter.Fill(dt);
                        return dt;
                    }
                }
            }
        }
    }
}