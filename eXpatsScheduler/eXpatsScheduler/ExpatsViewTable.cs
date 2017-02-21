using MySql.Data.MySqlClient;
using System;
using System.Collections;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;

namespace eXpatsScheduler
{
    internal class ExpatsViewTable
    {
        internal static void Start()
        {
            ArrayList dtList = new ArrayList();
            //R001_kod_stesen_kastam
            //DataTable dtStesenKastam = getStesenKastam();
            //dtList.Add(dtStesenKastam);
            //R002_kod_bandar
            //DataTable dtKodBandar = getKodBandar();
            //dtList.Add(dtKodBandar);
            //R003_kod_negeri
            //DataTable dtKodNegeri = getKodNegeri();
            //dtList.Add(dtKodNegeri);
            //R004_kod_negara
            //DataTable dtKodNegara= getKodNegara();
            //dtList.Add(dtKodNegara);
            //R005_kod_unit_ukuran
            //DataTable dtKodUnitUkuran= getKodUnitUkuran();
            //dtList.Add(dtKodUnitUkuran);
            //R006_kod_sektor
            //DataTable dtKodSektor = getKodSektor();
            //dtList.Add(dtKodSektor);
            //RD001_maklumat_permohonan
            //DataTable dtMaklumatPermohonan = getMaklumatPermohonan();
            //dtList.Add(dtMaklumatPermohonan);
            //RD002_maklumat_syarikat
            //DataTable dtMaklumatSyarikat= getMaklumatSyarikat();
            //dtList.Add(dtMaklumatSyarikat);
            //RD003_maklumat_premis
            //DataTable dtMaklumatPremis = getMaklumatPremis();
            //dtList.Add(dtMaklumatPremis);
            //RD005_maklumat_peralatan_import
            //DataTable dtMaklumatPeralatan = getMaklumatPeralatan();
            //dtList.Add(dtMaklumatPeralatan);
            //GenerateExcelFile(dtList);
            StoreIntoMOF(dtList);
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

        private static void GenerateExcelFile(ArrayList dtList)
        {
            string folderPath = ConfigurationSettings.AppSettings["ExcelLocation"].ToString();
            if (Directory.Exists(folderPath))
            {
                DeleteAllFile(folderPath);
            }
            foreach (DataTable dt in dtList)
            {
                ExcelFileHelper.GenerateExcelFile(dt, dt.TableName.ToString());

            }
        }
        private static void DeleteAllFile(string folderPath)
        {
            DirectoryInfo di = new DirectoryInfo(folderPath);

            foreach (FileInfo file in di.GetFiles())
            {
                file.Delete();
            }
            foreach (DirectoryInfo dir in di.GetDirectories())
            {
                dir.Delete(true);
            }
        }
        
        private static DataTable getStesenKastam()
        {
            DataTable output = new DataTable("R001_kod_stesen_kastam");
            string connectionString = @"Server=192.168.22.202;userid=itduser;password=Mdec@2016;Database=edfi2;Convert Zero Datetime=True";
            MySqlConnection conn = null;
            conn = new MySqlConnection(connectionString);
            conn.Open();

            try
            {

                StringBuilder sql = new StringBuilder();
                sql.AppendLine("SELECT * FROM R001_kod_stesen_kastam");
                MySqlDataAdapter returnVal = new MySqlDataAdapter(sql.ToString(), conn);
                returnVal.Fill(output);
            }
            catch (Exception)
            {

                throw;
            }
            conn.Close();
            return output;
        }
        private static DataTable getKodBandar()
        {
            DataTable output = new DataTable("R002_kod_bandar");
            string connectionString = @"Server=192.168.22.202;userid=itduser;password=Mdec@2016;Database=edfi2;Convert Zero Datetime=True";
            MySqlConnection conn = null;
            conn = new MySqlConnection(connectionString);
            conn.Open();

            try
            {

                StringBuilder sql = new StringBuilder();
                sql.AppendLine("SELECT * FROM R002_kod_bandar");
                MySqlDataAdapter returnVal = new MySqlDataAdapter(sql.ToString(), conn);
                returnVal.Fill(output);
            }
            catch (Exception)
            {

                throw;
            }
            conn.Close();
            return output;
        }

        private static DataTable getKodNegeri()
        {
            DataTable output = new DataTable("R003_kod_negeri");
            string connectionString = @"Server=192.168.22.202;userid=itduser;password=Mdec@2016;Database=edfi2;Convert Zero Datetime=True";
            MySqlConnection conn = null;
            conn = new MySqlConnection(connectionString);
            conn.Open();

            try
            {

                StringBuilder sql = new StringBuilder();
                sql.AppendLine("SELECT * FROM R003_kod_negeri");
                MySqlDataAdapter returnVal = new MySqlDataAdapter(sql.ToString(), conn);
                returnVal.Fill(output);
            }
            catch (Exception)
            {

                throw;
            }
            conn.Close();
            return output;
        }

        private static DataTable getKodNegara()
        {
            DataTable output = new DataTable("R004_kod_negara");
            string connectionString = @"Server=192.168.22.202;userid=itduser;password=Mdec@2016;Database=edfi2;Convert Zero Datetime=True";
            MySqlConnection conn = null;
            conn = new MySqlConnection(connectionString);
            conn.Open();

            try
            {

                StringBuilder sql = new StringBuilder();
                sql.AppendLine("SELECT * FROM R004_kod_negara");
                MySqlDataAdapter returnVal = new MySqlDataAdapter(sql.ToString(), conn);
                returnVal.Fill(output);
            }
            catch (Exception)
            {

                throw;
            }
            conn.Close();
            return output;
        }

        private static DataTable getKodSektor()
        {
            DataTable output = new DataTable("R006_kod_sektor");
            string connectionString = @"Server=192.168.22.202;userid=itduser;password=Mdec@2016;Database=edfi2;Convert Zero Datetime=True";
            MySqlConnection conn = null;
            conn = new MySqlConnection(connectionString);
            conn.Open();

            try
            {

                StringBuilder sql = new StringBuilder();
                sql.AppendLine("SELECT * FROM R006_kod_sektor");
                MySqlDataAdapter returnVal = new MySqlDataAdapter(sql.ToString(), conn);
                returnVal.Fill(output);
            }
            catch (Exception)
            {

                throw;
            }
            conn.Close();
            return output;
        }

        private static DataTable getKodUnitUkuran()
        {
            DataTable output = new DataTable("R005_kod_unit_ukuran");
            string connectionString = @"Server=192.168.22.202;userid=itduser;password=Mdec@2016;Database=edfi2;Convert Zero Datetime=True";
            MySqlConnection conn = null;
            conn = new MySqlConnection(connectionString);
            conn.Open();

            try
            {

                StringBuilder sql = new StringBuilder();
                sql.AppendLine("SELECT * FROM R005_kod_unit_ukuran");
                MySqlDataAdapter returnVal = new MySqlDataAdapter(sql.ToString(), conn);
                returnVal.Fill(output);
            }
            catch (Exception)
            {

                throw;
            }
            conn.Close();
            return output;
        }

        private static DataTable getMaklumatPermohonan()
        {
            DataTable output = new DataTable("RD001_maklumat_permohonan");
            string connectionString = @"Server=192.168.22.202;userid=itduser;password=Mdec@2016;Database=edfi2;Convert Zero Datetime=True";
            MySqlConnection conn = null;
            conn = new MySqlConnection(connectionString);
            conn.Open();

            try
            {

                StringBuilder sql = new StringBuilder();
                sql.AppendLine("SELECT * FROM RD001_maklumat_permohonan");
                MySqlDataAdapter returnVal = new MySqlDataAdapter(sql.ToString(), conn);
                returnVal.Fill(output);
            }
            catch (Exception)
            {

                throw;
            }
            conn.Close();
            return output;
        }

        private static DataTable getMaklumatSyarikat()
        {
            DataTable output = new DataTable("RD002_maklumat_syarikat");
            string connectionString = @"Server=192.168.22.202;userid=itduser;password=Mdec@2016;Database=edfi2;Convert Zero Datetime=True";
            MySqlConnection conn = null;
            conn = new MySqlConnection(connectionString);
            conn.Open();

            try
            {

                StringBuilder sql = new StringBuilder();
                sql.AppendLine("SELECT * FROM RD002_maklumat_syarikat");
                MySqlDataAdapter returnVal = new MySqlDataAdapter(sql.ToString(), conn);
                returnVal.Fill(output);
            }
            catch (Exception)
            {

                throw;
            }
            conn.Close();
            return output;
        }

        private static DataTable getMaklumatPremis()
        {
            DataTable output = new DataTable("RD003_maklumat_premis");
            string connectionString = @"Server=192.168.22.202;userid=itduser;password=Mdec@2016;Database=edfi2;Convert Zero Datetime=True";
            MySqlConnection conn = null;
            conn = new MySqlConnection(connectionString);
            conn.Open();

            try
            {

                StringBuilder sql = new StringBuilder();
                sql.AppendLine("SELECT * FROM RD003_maklumat_premis");
                MySqlDataAdapter returnVal = new MySqlDataAdapter(sql.ToString(), conn);
                returnVal.Fill(output);
            }
            catch (Exception)
            {

                throw;
            }
            conn.Close();
            return output;
        }

        private static DataTable getMaklumatPeralatan()
        {
            DataTable output = new DataTable("RD005_maklumat_peralatan_import");
            string connectionString = @"Server=192.168.22.202;userid=itduser;password=Mdec@2016;Database=edfi2;Convert Zero Datetime=True";
            MySqlConnection conn = null;
            conn = new MySqlConnection(connectionString);
            conn.Open();

            try
            {

                StringBuilder sql = new StringBuilder();
                sql.AppendLine("SELECT * FROM RD005_maklumat_peralatan_import");
                MySqlDataAdapter returnVal = new MySqlDataAdapter(sql.ToString(), conn);
                returnVal.Fill(output);
            }
            catch (Exception)
            {

                throw;
            }
            conn.Close();
            return output;
        }

    }
}