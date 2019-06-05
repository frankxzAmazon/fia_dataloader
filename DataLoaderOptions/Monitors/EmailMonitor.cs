using DataLoaderOptions.MicrosoftExchange;
using Microsoft.Exchange.WebServices.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Extensions;
using DataLoaderOptions.Readers;
namespace DataLoaderOptions.Monitors
{
    public class EmailMonitor
    {
        private StreamingSubscriptionConnection connection = null;
        private StreamingSubscriptionConnection.NotificationEventDelegate _onNewMail;
        private StreamingSubscriptionConnection.SubscriptionErrorDelegate _onDisconnect;
        private StreamingSubscriptionConnection.SubscriptionErrorDelegate _onError;
        public EmailMonitor ()
        {
            _onNewMail = new StreamingSubscriptionConnection.NotificationEventDelegate(OnNewMail);
            _onDisconnect = new StreamingSubscriptionConnection.SubscriptionErrorDelegate(OnDisconnect);
            _onError = new StreamingSubscriptionConnection.SubscriptionErrorDelegate(OnError);
        }
        public ExchangeMailbox Mailbox { get; set; }
        public string MailboxFolderName { get; set; }
        public string OutputFolder { get; set; }
        public string SubjectSubString { get; set; }
        public string AttachmentSubString { get; set; }
        public ExchangeService Service => Mailbox.Service;
        public IFileReader Reader { get; set; }
        public Action<DataTable> OnChange { get; set; }

        public void BeginMonitoring()
        {
            FolderId subscriptionFolder = Mailbox.GetFolderID(MailboxFolderName);
            StreamingSubscription streamingsubscription = Service.SubscribeToStreamingNotifications(new FolderId[] { subscriptionFolder },
                 EventType.NewMail);
            if (connection == null) CreateStreamingConnection();
            connection.AddSubscription(streamingsubscription);
        }
        public void EndMonitoring()
        {
            connection.OnDisconnect -= _onDisconnect;
            connection.OnSubscriptionError -= _onError;
            connection.Close();
        }
        private void CreateStreamingConnection()
        {
            connection = new StreamingSubscriptionConnection(Service, 30);
            connection.OnNotificationEvent += new StreamingSubscriptionConnection.NotificationEventDelegate(OnNewMail);
            connection.OnDisconnect += new StreamingSubscriptionConnection.SubscriptionErrorDelegate(OnDisconnect);
            connection.OnSubscriptionError += new StreamingSubscriptionConnection.SubscriptionErrorDelegate(OnError);
            connection.Open();
        }
        private void OnNewMail(object sender, NotificationEventArgs args)
        {
            var newMails = from e in args.Events.OfType<ItemEvent>()
                           where e.EventType == EventType.NewMail
                           select e.ItemId;
            if (newMails.Count() > 0)
            {
                var response = Service.BindToItems(newMails,
                   new PropertySet(BasePropertySet.IdOnly, ItemSchema.DateTimeReceived, ItemSchema.Subject, ItemSchema.Attachments));
                var items = response.Select(itemResponse => itemResponse.Item).Where(x=> x.Subject.CaseInsensitiveContains(SubjectSubString));
                var files = items.SelectMany(x => x.Attachments)
                                    .Where(x => (x is FileAttachment) && x.Name.CaseInsensitiveContains(AttachmentSubString))
                                    .Select(x=> x as FileAttachment).ToList();
                var filePaths = DownloadAttachments(files);
                if(filePaths.Count > 0 )
                {
                    foreach(var filepath in filePaths)
                    {
                        Reader.FilePath = filepath;
                        var dataTable = Reader.GetFilledDataTable(Readers.OnError.UseNullValue);
                        OnChange.Invoke(dataTable);
                    }
                }
            }
        }
        private List<string> DownloadAttachments(List<FileAttachment> attachments)
        {
            List<string> filePaths = new List<string>();
            foreach (var attachment in attachments)
            {
                var filePath = OutputFolder + DateTime.Now.ToString("yyyyMMdd_hhmmss") + attachment.Name;
                attachment.Load(filePath);
                filePaths.Add(filePath);
            }
            return filePaths;
        }
        private void OnDisconnect(object sender, SubscriptionErrorEventArgs args)
        {
            connection.Open();
        }
        private void OnError(object sender, SubscriptionErrorEventArgs args)
        {
            connection.Open();
        }
    }
}
