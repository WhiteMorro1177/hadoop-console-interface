using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WebHDFS;

namespace HaboopaConsole
{
	internal class Program
	{
		private static string path = "/";

		private static string username = "";
		private static string host = "";
		private static string port = "";

		static async Task Main(string[] args)
		{
			// extract args
			host = args[0];
			port = args[1];
			username = args[2];

			string connectionString = $"http://{host}:{port}/webhdfs/v1/user/{username}";


			string command = RequestCommand();
			while (command != "exit")
			{
				string[] extractedCommand = command.Split(' ');
				extractedCommand.ToList().ForEach(x => Console.Write(x + " "));
				Console.WriteLine();

				switch (extractedCommand[0])
				{
					case "cd":
						CallCDCommand(extractedCommand[1]);
						break;
					case "ls":
						await CallLSCommand(extractedCommand[1]);
						break;
				}



				command = RequestCommand();
			}
		}

		private static string RequestCommand()
		{
			string template = $"{username}@{host}:/{path} > ";
			Console.Write(template);
			
			string command = Console.ReadLine();
			if (command.StartsWith(template)) command = command.Substring(template.Length);

			return command;
		}

		public static void CallCDCommand(string newPath)
		{
			if (string.IsNullOrEmpty(newPath)) return;
			
			if (newPath.StartsWith("/")) path = newPath;
			else path = $"/{newPath}";
		}

        public static async Task CallLSCommand(string parameter)
        {
            WebHDFSClient webHDFSClient = new WebHDFSClient("http://10.241.167.184/webhdfs/v1/home/share/hadoop-fs");

            var res = await webHDFSClient.ListStatus("home/share/hadoop-fs");
            Console.WriteLine($"res: " + res);
        }
    }
}
