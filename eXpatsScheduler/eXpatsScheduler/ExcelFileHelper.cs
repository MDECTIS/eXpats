using ClosedXML.Excel;
using System;
using System.Configuration;
using System.Data;
using System.IO;

namespace eXpatsScheduler
{
    internal class ExcelFileHelper
    {
        internal static void GenerateExcelFile(DataTable dt, string FileName)
        {
            
            string folderPath = ConfigurationSettings.AppSettings["ExcelLocation"].ToString();
            if (!Directory.Exists(folderPath))
            {
                Directory.CreateDirectory(folderPath);
            }
         
            string ExcelFile = "";
            ExcelFile = ConfigurationSettings.AppSettings["ExcelLocation"].ToString() + FileName + "_" + DateTime.Now.ToString("ddMMyyyy") + ".xlsx";
            ConvertToExcel(dt, ExcelFile, FileName);
        }

   

        private static void ConvertToExcel(DataTable dt, string ExcelFile, string FileName)
        {
            using (XLWorkbook wb = new XLWorkbook())
            {
                wb.Worksheets.Add(dt, FileName);
                wb.SaveAs(ExcelFile);
            }
        }
    }
}