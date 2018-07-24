using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SATPOC
{
    interface IDocumentReader
    {
        List<String> LoadDocuments();

        List<String> TransformDocuments();
    }
}
