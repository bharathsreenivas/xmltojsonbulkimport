using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using Microsoft.Azure.Documents;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Formatting = Newtonsoft.Json.Formatting;

namespace SATPOC
{
    internal static class Utility
    {
        internal static List<String> XmlToJSON(List<String> xmlDocuments)
        {
            List<String> jsonDocuments = new List<String>();
            foreach (String document in xmlDocuments)
            {
                XmlDocument xmlDoc = new XmlDocument();
                xmlDoc.LoadXml(document);
                string jsonText = JsonConvert.SerializeXmlNode(xmlDoc, Formatting.Indented);
                var jObject = JObject.Parse(jsonText);
                string pk = jObject["cfdi:Comprobante"]["cfdi:Emisor"]["@Rfc"].ToString();
                jObject.Add("pk", pk);
                jObject.Add("id", System.Guid.NewGuid().ToString());
                jsonDocuments.Add(jObject.ToString());
            }

            return jsonDocuments;
        }

        internal static List<Tuple<string, string, string>> XmlToTuples(List<String> xmlDocuments)
        {
            List<Tuple<string, string, string>> mapping = new List<Tuple<string, string, string>>();

            foreach (String document in xmlDocuments)
            {
                try
                {
                    XmlDocument xmlDoc = new XmlDocument();
                    xmlDoc.LoadXml(document);
                    string jsonText = JsonConvert.SerializeXmlNode(xmlDoc, Formatting.Indented);
                    var jObject = JObject.Parse(jsonText);
                    string emisor = jObject["cfdi:Comprobante"]["cfdi:Emisor"]["@rfc"].ToString();
                    string receptor = jObject["cfdi:Comprobante"]["cfdi:Receptor"]["@rfc"].ToString();
                    string cfdiinvoice = jObject["cfdi:Comprobante"]["cfdi:Complemento"]["tfd:TimbreFiscalDigital"]["@UUID"].ToString();
                    mapping.Add(new Tuple<string, string, string>(emisor, cfdiinvoice, "E"));
                    mapping.Add(new Tuple<string, string, string>(cfdiinvoice, receptor, "R"));
                }
                catch (Exception ex)
                {
                     Console.WriteLine("Input data in wrong format \n" + ex.ToString());
                } 
            }

            return mapping;
        }
    }
}
