using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SharePoint.Client;
using System.IO;
using System.Net;
using System.Security;
using Extensions;
namespace DataLoaderOptions.MicrosoftExchange
{
    public class SharepointSite
    {
        public string Url { get; set; }
        public string SharePointFolderName { get; set; }
        public string DownloadPath { get; set; }
        public Func<string, bool> CheckFile { get; set; }
        public void DownloadFiles ()
        {
            using (var clientContext = new ClientContext(Url))
            {
                SecureString password = new SecureString();
                password.AppendChar('R');
                password.AppendChar('u');
                password.AppendChar('s');
                password.AppendChar('s');
                password.AppendChar('s');
                password.AppendChar('m');
                password.AppendChar('i');
                password.AppendChar('t');
                password.AppendChar('h');
                password.AppendChar('!');

                clientContext.Credentials = new SharePointOnlineCredentials("almserviceaccount@delawarelife.com", password);
                var qry = new CamlQuery();
                qry.ViewXml = "<View Scope='RecursiveAll'>" +
                                         "<Query>" +
                                             "<Where>" +
                                                   "<Eq>" +
                                                        "<FieldRef Name='FSObjType' />" +
                                                        "<Value Type='Integer'>0</Value>" +
                                                   "</Eq>" +
                                            "</Where>" +
                                          "</Query>" +
                                       "</View>";
                var list = clientContext.Web.Lists.GetByTitle(SharePointFolderName);
                var items = list.GetItems(qry);
                clientContext.Load(items);
                clientContext.ExecuteQuery();
                foreach (var item in items)
                {
                    var fileRef = (string)item["FileRef"];
                    var fileName = Path.GetFileName(fileRef);
                    if (CheckFile(fileName))
                    {
                        var filePath = Path.Combine(DownloadPath, fileName);
                        var fileInfo = Microsoft.SharePoint.Client.File.OpenBinaryDirect(clientContext, fileRef);
                        using (var fileStream = System.IO.File.Create(filePath))
                        {
                            fileInfo.Stream.CopyTo(fileStream);
                        }
                    }
                }
            }
        }

    }
}
