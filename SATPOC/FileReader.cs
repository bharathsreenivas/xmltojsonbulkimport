using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SATPOC
{
    internal class FileReader : IDocumentReader
    {
        internal string folderName;
        internal FileReader(string folderName)
        {
            this.folderName = folderName;
        }
        public List<string> LoadDocuments()
        {
            List<String> documentsRead = Directory.EnumerateFiles(folderName).Select(File.ReadAllText).ToList();
            return documentsRead;
        }

        public List<string> TransformDocuments()
        {
            throw new NotImplementedException();
        }
    }
}
