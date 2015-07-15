using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using StackOverflowTagServer;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Web;
using System.Web.Http;
using System.Web.Mvc;
using System.Web.Optimization;
using System.Web.Routing;

namespace Server
{
    public class WebApiApplication : System.Web.HttpApplication
    {
        internal static Lazy<TagServer> TagServer = new Lazy<TagServer>(() =>
            {
                return CreateTagServer();
            }, LazyThreadSafetyMode.ExecutionAndPublication);

        protected void Application_Start()
        {
            AreaRegistration.RegisterAllAreas();
            GlobalConfiguration.Configure(WebApiConfig.Register);
            FilterConfig.RegisterGlobalFilters(GlobalFilters.Filters);
            RouteConfig.RegisterRoutes(RouteTable.Routes);
            BundleConfig.RegisterBundles(BundleTable.Bundles);

            // For initialisation to happen here, rather than waiting for the first request
            try
            {
                var temp = WebApiApplication.TagServer.Value;
            }
            catch (Exception ex)
            {
                Trace.Write(ex);
            }
        }

        private static TagServer CreateTagServer()
        {
            var dataFolder = HttpContext.Current.Server.MapPath("~/Data");
            Trace.WriteLine("Data folder: " + dataFolder);
            ListFolderInfo(dataFolder);
            Trace.WriteLine("Finished listing contents of Data folder: " + dataFolder);

            var questionsFileName = "Questions-NEW.bin";
            var questionsPath = Path.Combine(dataFolder, questionsFileName);
            if (File.Exists(questionsPath) == false)
            {
                if (Directory.Exists(dataFolder) == false)
                    Directory.CreateDirectory(dataFolder);

                DownloadDataFiles(dataFolder);
            }

            Trace.WriteLine("Data folder: " + dataFolder);
            ListFolderInfo(dataFolder);
            Trace.WriteLine("Finished listing contents of Data folder: " + dataFolder);

            //return StackOverflowTagServer.TagServer.CreateFromFile(questionsPath);
            var questions = StackOverflowTagServer.TagServer.GetRawQuestionsFromDisk(dataFolder, questionsFileName);
            return StackOverflowTagServer.TagServer.CreateFromSerialisedData(questions, dataFolder);
        }

        private static void DownloadDataFiles(string dataFolder)
        {
            Trace.WriteLine("StorageConnectionString: " + CloudConfigurationManager.GetSetting("StorageConnectionString"));
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference("data");
            ListBlobsInContainer(container);

            foreach (IListBlobItem item in container.ListBlobs(null, false))
            {
                //if (item.GetType() == typeof(CloudBlockBlob) && item.Uri.ToString().EndsWith(".zip"))
                if (item.GetType() == typeof(CloudBlockBlob))
                {
                    CloudBlockBlob blob = (CloudBlockBlob)item;
                    //Trace.WriteLine(string.Format("Found blob {0}", blob.Name));

                    //if (blob.Name != "intermediate-AnswerCount.zip")
                    //if (blob.Name != "Questions-NEW.zip")
                    if (blob.Name != "Questions-NEW.bin")
                        continue;
                    Trace.WriteLine(string.Format("Found blob {0}", blob.Name));

                    var blobOutput = Path.Combine(dataFolder, blob.Name);
                    DownloadBlob(blob, blobOutput);

                    //var decompressedFile = Path.Combine(dataFolder, Path.GetFileNameWithoutExtension(blobOutput) + ".bin");
                    //if (File.Exists(decompressedFile))
                    //    File.Delete(decompressedFile);
                    //var decompressTimer = Stopwatch.StartNew();
                    //ZipFile.ExtractToDirectory(blobOutput, dataFolder);
                    //decompressTimer.Stop();
                    //Trace.WriteLine(string.Format("Took {0} to decompress to {1} ({2})",
                    //                decompressTimer.Elapsed, Path.GetFileName(blobOutput), Path.GetFileName(decompressedFile)));
                    //var info = new FileInfo(decompressedFile);
                    //Trace.WriteLine(string.Format("Decompressed file {0}, {1:N0} bytes", Path.GetFileName(decompressedFile), new FileInfo(decompressedFile).Length));
                }
            }
        }

        private static void DownloadBlob(CloudBlockBlob blob, string outputPath, bool deleteIfAlreadyExists = false)
        {
            // Save blob contents to a file.
            if (deleteIfAlreadyExists && File.Exists(outputPath))
                File.Delete(outputPath);

            using (var fileStream = File.OpenWrite(outputPath))
            {
                var timer = Stopwatch.StartNew();
                blob.DownloadToStream(fileStream);
                timer.Stop();
                Trace.WriteLine(string.Format("Took {0} to download {1:N0} bytes", timer.Elapsed, blob.Properties.Length));
                Trace.WriteLine(string.Format("File {0}, Info (on disk) {1:N0} bytes", Path.GetFileName(outputPath), new FileInfo(outputPath).Length));
            }
        }

        private static void ListFolderInfo(string folderName)
        {
            Process p = new Process();
            p.StartInfo.FileName = "cmd.exe";
            p.StartInfo.Arguments = "/c dir " + folderName;
            p.StartInfo.UseShellExecute = false;
            p.StartInfo.RedirectStandardOutput = true;
            p.Start();

            p.WaitForExit();
            string output = p.StandardOutput.ReadToEnd();
            //p.WaitForExit();
            Trace.WriteLine(output);
        }

        private static void ListBlobsInContainer(CloudBlobContainer container)
        {
            // Loop over items within the container and output the length and URI.
            foreach (IListBlobItem item in container.ListBlobs(null, false))
            {
                if (item.GetType() == typeof(CloudBlockBlob))
                {
                    CloudBlockBlob blob = (CloudBlockBlob)item;
                    Trace.WriteLine(string.Format("Block blob of length {0}: {1}", blob.Properties.Length, blob.Uri));
                }
                else if (item.GetType() == typeof(CloudPageBlob))
                {
                    CloudPageBlob pageBlob = (CloudPageBlob)item;
                    Trace.WriteLine(string.Format("Page blob of length {0}: {1}", pageBlob.Properties.Length, pageBlob.Uri));
                }
                else if (item.GetType() == typeof(CloudBlobDirectory))
                {
                    CloudBlobDirectory directory = (CloudBlobDirectory)item;
                    Trace.WriteLine(string.Format("Directory: {0}", directory.Uri));
                }
            }
        }
    }
}
