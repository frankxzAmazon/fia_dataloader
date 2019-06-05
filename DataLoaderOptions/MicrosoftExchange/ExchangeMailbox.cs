using Microsoft.Exchange.WebServices.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Extensions;

namespace DataLoaderOptions.MicrosoftExchange
{
    public class ExchangeMailbox
    {
        
        private string emailaddress;
        public ExchangeMailbox()
        {
            IsSignedIn = false;
        }
        public bool IsSignedIn { get; private set; }
        public ExchangeService Service { get; } = new ExchangeService();
        public bool SignIn()
        {
            //string[] loginInfo = new Username_Password().Login();
            //if (loginInfo[1] == "Cancel")
            //    return false;
            Service.Credentials = new WebCredentials("almserviceaccount@delawarelife.com", "Russsmith!");
            //try
            //{
                emailaddress = "almserviceaccount@delawarelife.com";
                Service.AutodiscoverUrl(emailaddress, RedirectionUrlValidationCallback);
                IsSignedIn = true;
                return true;
            //}
            //catch
            //{
            //    loginInfo = new Username_Password().Login();
            //    if (loginInfo[1] == "Cancel")
            //        return false;
            //    Service.Credentials = new WebCredentials(loginInfo[0], loginInfo[1]);
            //    try
            //    {
            //        emailaddress = loginInfo[0];
            //        Service.AutodiscoverUrl(emailaddress, RedirectionUrlValidationCallback);
            //        IsSignedIn = true;
            //        return true;
            //    }
            //    catch
            //    {
            //        return false;
            //    }
            //}
        }
        public List<FileAttachment> GetAttachments(List<Item> findResults, string attachmentName)
        {
            List<FileAttachment> attachments = new List<FileAttachment>();
            foreach (Item item in findResults)
            {
                EmailMessage message = EmailMessage.Bind(Service, item.Id, new PropertySet(ItemSchema.Attachments, ItemSchema.DateTimeReceived));
                foreach (Attachment attachment in message.Attachments)
                {
                    if (attachment.Name.CaseInsensitiveContains(attachmentName))
                    {
                        if (attachment is FileAttachment)
                        {
                            attachments.Add(attachment as FileAttachment);
                        }
                    }
                }
            }
            return attachments;
        }
        public List<FileAttachment> GetAttachments(List<Item> findResults, string[] attachmentNames)
        {
            List<FileAttachment> attachments = new List<FileAttachment>();
            int? i;
            foreach (Item item in findResults)
            {
                EmailMessage message = EmailMessage.Bind(Service, item.Id, new PropertySet(ItemSchema.Attachments, ItemSchema.DateTimeReceived));
                foreach (Attachment attachment in message.Attachments)
                {
                    foreach (var attachmentName in attachmentNames)
                    {
                        if (attachment.Name.CaseInsensitiveContains(attachmentName))
                        {
                            if (attachment is FileAttachment)
                            {
                                attachments.Add(attachment as FileAttachment);
                            }
                        }
                    }
                }
            }
            return attachments;
        }
        public void MoveFiles(List<Item> findResults, string attachmentName, FolderId moveFolder)
        {
            foreach (Item item in findResults)
            {
                bool toMove = false;
                EmailMessage message = EmailMessage.Bind(Service, item.Id, new PropertySet(ItemSchema.Attachments, ItemSchema.DateTimeReceived));
                foreach (Attachment attachment in message.Attachments)
                {
                    if (attachment.Name.CaseInsensitiveContains(attachmentName))
                    {
                        if (attachment is FileAttachment)
                        {
                            toMove = true;
                            break;
                        }
                    }
                }
                if (toMove)
                {
                    message.Move(moveFolder);
                }
            }
        }
        public void MoveFiles(List<Item> findResults, FolderId moveFolder)
        {
            foreach (Item item in findResults)
            {
                EmailMessage message = EmailMessage.Bind(Service, item.Id, new PropertySet(ItemSchema.Attachments, ItemSchema.DateTimeReceived));
                message.Move(moveFolder);
            }
        }
        public List<Item> FilterFolder(FolderId inputid, string emailSubject = null, DateTime? firstDate = null, DateTime? lastDate = null)
        {
            List<Item> items = new List<Item>();
            FindItemsResults<Item> results;
            int? pageOffset = 0;
            PropertySet propSet = new PropertySet
            {
                ItemSchema.HasAttachments,
                ItemSchema.Subject,
                ItemSchema.DateTimeReceived,
                ItemSchema.Attachments,
                ItemSchema.ParentFolderId
            };
            do
            {
                ItemView view = new ItemView(1000, pageOffset ?? 0)
                {
                    PropertySet = new PropertySet(BasePropertySet.IdOnly),
                    Traversal = ItemTraversal.Shallow
                };
                view.OrderBy.Add(ItemSchema.DateTimeReceived, SortDirection.Descending);
                List<SearchFilter> searchFilterCollection = new List<SearchFilter>();
                if (!string.IsNullOrEmpty(emailSubject))
                {
                    searchFilterCollection.Add(new SearchFilter.ContainsSubstring(ItemSchema.Subject, emailSubject, ContainmentMode.Substring, ComparisonMode.IgnoreCase));
                }
                searchFilterCollection.Add(new SearchFilter.IsGreaterThanOrEqualTo(ItemSchema.DateTimeReceived, firstDate ?? DateTime.MinValue));
                searchFilterCollection.Add(new SearchFilter.IsLessThanOrEqualTo(ItemSchema.DateTimeReceived, lastDate ?? DateTime.MaxValue));
                SearchFilter searchFilter = new SearchFilter.SearchFilterCollection(LogicalOperator.And, searchFilterCollection);
                results = Service.FindItems(inputid, searchFilter, view);
                pageOffset = results.NextPageOffset;
                if (results.Items.Count > 0)
                {
                    Service.LoadPropertiesForItems(results.Items, propSet);
                }
                items.AddRange(results.Items);
            } while (pageOffset != null);
            return items;
        }
        public FolderId GetFolderID(string foldername,string mailboxName = null)
        {
            
            var folderId = mailboxName == null ? new FolderId(WellKnownFolderName.Root): new FolderId(WellKnownFolderName.Root, mailboxName);
            FolderView view = new FolderView(1000)
            {
                PropertySet = new PropertySet(BasePropertySet.IdOnly)
            };
            view.PropertySet.Add(FolderSchema.DisplayName);
            view.Traversal = FolderTraversal.Deep;
            FindFoldersResults findFolderResults = Service.FindFolders(folderId, view);
            foreach (Folder f in findFolderResults)
            {
                //show folderId of the folder "test"
                if (f.DisplayName.Equals(foldername, StringComparison.InvariantCultureIgnoreCase))
                    return f.Id;
            }
            return WellKnownFolderName.Inbox;
        }

        private static bool RedirectionUrlValidationCallback(string redirectionUrl)
        {
            // The default for the validation callback is to reject the URL.
            bool result = false;

            Uri redirectionUri = new Uri(redirectionUrl);

            // Validate the contents of the redirection URL. In this simple validation
            // callback, the redirection URL is considered valid if it is using HTTPS
            // to encrypt the authentication credentials. 
            if (redirectionUri.Scheme == "https")
            {
                result = true;
            }
            return result;
        }

    }
}
