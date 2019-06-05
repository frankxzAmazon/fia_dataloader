using Microsoft.Exchange.WebServices.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Extensions;
namespace DataLoaderOptions.MicrosoftExchange
{
    class AttachmentSearch
    {
        private string _outputPath;
        public AttachmentSearch()
        {
            SearchFolder = WellKnownFolderName.Inbox;
            MoveFolder = WellKnownFolderName.Inbox;
        }
        public FolderId SearchFolder { get; set; }
        public FolderId MoveFolder { get; set; }
        public ExchangeMailbox Exchange { get; set; }
        public string OutputPath
        {
            get { return _outputPath; }
            set { _outputPath = value.CheckFolderPath(); }
        }

        public void SetSearchFolder(string folderName, string mailboxName = null)
        {
            if (string.IsNullOrWhiteSpace(folderName))
            {
                SearchFolder = WellKnownFolderName.Inbox;
            }
            else
            {
                SearchFolder = Exchange.GetFolderID(folderName, mailboxName);
            }
        }
        public void SetMoveFolder(string folderName, string mailboxName = null)
        {
            if (string.IsNullOrWhiteSpace(folderName))
            {
                MoveFolder = WellKnownFolderName.Inbox;
            }
            else
            {
                MoveFolder = Exchange.GetFolderID(folderName, mailboxName);
            }
        }
        public void DownloadAttachments(string attachmentName, string subjectFiler = null, DateTime? firstDate = null, DateTime? lastDate = null)
        {
            List<Item> findResults;
            findResults = Exchange.FilterFolder(SearchFolder, subjectFiler, firstDate, lastDate);
            List<FileAttachment> attachments = Exchange.GetAttachments(findResults, attachmentName);
            foreach (var attachment in attachments)
            {
                attachment.Load(OutputPath + DateTime.Now.ToString("yyyyMMdd_hhmmss") + attachment.Name);
            }
            Exchange.MoveFiles(findResults, MoveFolder);
        }
        public void DownloadAttachments(string[] attachmentNames, string subjectFiler = null, DateTime? firstDate = null, DateTime? lastDate = null)
        {
            List<Item> findResults;
            findResults = Exchange.FilterFolder(SearchFolder, subjectFiler, firstDate, lastDate);
            List<FileAttachment> attachments = Exchange.GetAttachments(findResults, attachmentNames);
            foreach (var attachment in attachments)
            {
                attachment.Load(OutputPath + DateTime.Now.ToString("yyyyMMdd_hhmmss") + attachment.Name);
            }
            Exchange.MoveFiles(findResults, MoveFolder);
        }
        public void MoveFiles(string subjectFiler = null)
        {
            List<Item> findResults = Exchange.FilterFolder(SearchFolder, subjectFiler);
            Exchange.MoveFiles(findResults, MoveFolder);
        }
    }
}
