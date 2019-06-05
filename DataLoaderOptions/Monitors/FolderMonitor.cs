using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using Extensions;
using System.Data;
using DataLoaderOptions.Readers;

namespace DataLoaderOptions.Monitors
{
    public class FolderMonitor
    {
        private readonly FileSystemWatcher watcher = new FileSystemWatcher();
        private FileSystemEventHandler eventHandler;
        public FolderMonitor(string path, string filename)
        {
            PathToMonitor = path.CheckFolderPath();
            FileNameSubstring = filename;
            eventHandler += new FileSystemEventHandler(OnNew);
        }
        public string PathToMonitor { get; set; }
        public string FileNameSubstring { get; set; }
        public string OutputPath { get; set; }
        public IFileReader Reader {get;set;}
        public Action<DataTable> OnChange { get; set; }
        public void BeginMonitoring()
        {
            watcher.Path = PathToMonitor;
            watcher.NotifyFilter = NotifyFilters.LastWrite;
            watcher.Filter = "*.*";
            watcher.Changed += eventHandler;
            watcher.EnableRaisingEvents = true;
        }
        public void CancelMonitoring()
        {
            watcher.Path = PathToMonitor;
            watcher.NotifyFilter = NotifyFilters.LastWrite;
            watcher.Filter = "*.*";
            watcher.Changed -= eventHandler;
            watcher.EnableRaisingEvents = false;
        }
        private void OnNew(object sender, FileSystemEventArgs args)
        {
            string name = Path.GetFileNameWithoutExtension(args.FullPath);
            if(name.CaseInsensitiveContains(FileNameSubstring))
            {
                while (!IsFileReady(args.FullPath)) { Thread.Sleep(1000); }
                Reader.FilePath = args.FullPath;
                var dataTable = Reader.GetFilledDataTable(OnError.UseNullValue);
                OnChange.Invoke(dataTable);
                if(!string.IsNullOrWhiteSpace(OutputPath))
                {
                    File.Copy(args.FullPath, OutputPath + Path.GetFileName(args.FullPath));
                }
            }
        }
        private bool IsFileReady(string fullpath)
        {
            try
            {
                File.Copy(fullpath, OutputPath + Path.GetFileName(fullpath));
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}
