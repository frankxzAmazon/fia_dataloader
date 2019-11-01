using DataLoaderOptions.Loaders;
using System;
using System.Linq;

namespace DataLoaderOptions
{
    class Program
    {
        static void Main(string[] args)
        {
            bool toLoad = true;
            bool useDevDB = false;


            // This #if DEBUG region ensure that we only run this block and set the arguments manually if we are in debug mode
#if DEBUG
            // If we're in debug mode, then defualt to not wanting to load the data unless the /loadOn parameter is specified.
            toLoad = false;

            // All the possible run parameters
            var all_loaders = new string[] { "aladdin", "dlicnbfia", "bc", "baml", "bnp", "cs", "jp", /* "citi", "citiglac", */ "db",
                                             "gs", "ms", "wf", "keyport", "sundex", "elic", "sntl", "glac", "gisassets", "nac",
                                             "iap", "vol", "div", "rates", "treasury","aladdin_api" ,"dlicnbfia10yrc","dlicnbfia7yrs"};

            // Current parameters
            args = new string[] { "/loadOn", "elic" };

            // In case we need to run all the loaders at once, uncomment the following line:
            //args = all_loaders;
#endif

            foreach (var arg in args)
            {
                var test = arg.ToLowerInvariant();

                // Handle the optional /loadOff and /loadOn parameters
                if (args.Any(a => a.ToLowerInvariant() == "/loadon")) toLoad = true;
                if (args.Any(a => a.ToLowerInvariant() == "/loadoff")) toLoad = false;

                // Handle the optional /dev parameter
                if (args.Any(a => a.ToLowerInvariant() == "/dev"))
                {
                    useDevDB = true;
                }


                IDataLoader loader;
                switch (arg.ToLowerInvariant())
                {
                    case "dlicnbfia":
                        loader = new DataLoaderDLICNbFIA();
                        break;
                    case "dlicnbfia10yrc":
                        loader = new DataLoaderDLICNbFIA10YrC();
                        break;
                    case "dlicnbfia7yrs":
                        loader = new DataLoaderDLICNbFIA7YrS();
                        break;
                    //case "dlicasset":
                    //    DataLoaderDLICAsset dlicAsset = new DataLoaderDLICAsset();
                    //    dlicAsset.LoadToSql();
                    //    break;
                    case "bc":
                        loader = new DataLoaderBarclays();
                        break;
                    case "baml":
                        loader = new DataLoaderBAML();
                        break;
                    case "bnp":
                        loader = new DataLoaderBNP();
                        break; ;
                    case "cs":
                        loader = new DataLoaderCS();
                        break;
                    case "jp":
                        loader = new DataLoaderJP();
                        break;
                    case "citi":
                        loader = new DataLoaderCiti();
                        break;
                    case "citiglac":
                        loader = new DataLoaderCitiGlac();
                        break;
                    case "db":
                        loader = new DataLoaderDB();
                        break;
                    case "gs":
                        loader = new DataLoaderGS();
                        break;
                    case "ms":
                        loader = new DataLoaderMS();
                        break;
                    case "wf":
                        loader = new DataLoaderWF();
                        break;
                    case "keyport":
                        loader = new DataLoaderKeyPort();
                        break;
                    case "sundex":
                        loader = new DataLoaderSundex();
                        break;
                    case "elic":
                        loader = new DataLoaderELIC();
                        break;
                    case "sntl":
                        loader = new DataLoaderSNTL();
                        break;
                    case "glac":
                        loader = new DataLoaderGLAC();
                        break;
                    case "glactest":
                        loader = new DataLoaderGLAC_old();
                        break;
                    case "gisassets":
                        loader = new DataLoaderGISAssets();
                        break;
                    case "nac":
                        loader = new DataLoaderNAC();
                        break;
                    case "iap":
                        loader = new DataLoaderIAP();
                        break;
                    case "vol":
                        loader = new DataLoaderBloombergVol();
                        break;
                    case "div":
                        loader = new DataLoaderBloombergDiv();
                        break;
                    case "rates":
                        loader = new DataLoaderBloombergIndex();
                        loader.ToLoad = toLoad;
                        loader.LoadToSql();
                        loader = new DataLoaderBloombergRates();
                        break;
                    case "treasury":
                        loader = new DataLoaderBloombergTreasury();
                        break;
                    case "aladdin":
                        loader = new DataLoaderAladdin();
                        break;
                    case "aladdin_api":
                        loader = new DataLoaderAladdin_APIReader();
                        break;
                    case "dlicratesetting":
                        loader = new DataLoaderDLICRateSetting();
                        break;
                    case "/pause":
                        loader = null;
                        break;
                    default:
                        //RunAll();
                        loader = null;
                        break;
                }

                if (loader != null)
                {
                    loader.ToLoad = toLoad;
                    loader.useDevDB = useDevDB;
                    loader.LoadToSql();
                }
            }
            if (args.Any(a => a == "/pause"))
            {
                Console.Write("Waiting for key press to exit...");
                Console.ReadKey();
            }
        }
    }
}
