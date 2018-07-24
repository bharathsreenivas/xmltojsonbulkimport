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
    }
}
