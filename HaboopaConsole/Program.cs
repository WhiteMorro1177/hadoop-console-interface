
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using WebHDFS;

namespace HaboopaConsole
{
	internal class Program
	{
		private static string PATH = "/";

		private static string USERNAME = "";
		private static string HOST = "";
		private static string PORT = "";
		private static string URL = "";

		private static HttpClient httpClient;

		static async Task Main(string[] args)
		{
			httpClient = new HttpClient();
			if (args.Length == 0) return;
			// extract args
			HOST = args[0];
			PORT = args[1];
			USERNAME = args[2];

			URL = $"http://{HOST}:{PORT}/webhdfs/v1/user/{USERNAME}";

			string command = RequestCommand();
			while (command != "exit")
			{
				string[] extractedCommand = command.Split(' ');
				Console.WriteLine();

				switch (extractedCommand[0])
				{
					case "cd":
						await CD(extractedCommand[1]);
						break;
					case "ls":
						if (extractedCommand.Length > 1) await LS(extractedCommand[1]);
						else await LS();
						break;
				}

				command = RequestCommand();
			}
		}

		private static string RequestCommand()
		{
			string template = $"{USERNAME}@{HOST}:{PATH} > ";
			Console.Write(template);

			string command = Console.ReadLine();
			if (command.StartsWith(template)) command = command.Substring(template.Length);

			return command;
		}

		public static async Task CD(string newPath)
		{
			string pathBuffer = PATH;

			if (string.IsNullOrEmpty(newPath)) return;
			if (newPath == "..")
			{
				string[] splittedPath = PATH.Split(new[] { "/" }, StringSplitOptions.RemoveEmptyEntries);
				Array.Resize(ref splittedPath, splittedPath.Length - 1);

				PATH = "/" + string.Join("/", splittedPath);
			}
			else
			{
				if (newPath.StartsWith("/") || PATH == "/") PATH = $"/{newPath}";
				else PATH += $"/{newPath}";
			}
			if (PATH.StartsWith("//")) PATH = PATH.Replace("//", "/");

			bool isDirectoryExists = await CheckDirectory();
			if (!isDirectoryExists) PATH = pathBuffer;
		}

		private static async Task<bool> CheckDirectory(string path = null)
		{
			string pathToCheck = "";

			if (path == null) pathToCheck = PATH;
			else pathToCheck = path;

			
			var response = await httpClient.GetAsync(URL + pathToCheck + "?op=GETFILESTATUS");
			if (response.StatusCode == HttpStatusCode.OK) return true;
			else return false;
		}

		public static async Task LS(string parameter = null)
		{
			string pathToCheck = "";
			if (parameter == null) pathToCheck = PATH;
			else
			{
				if (parameter.StartsWith("/")) pathToCheck = parameter;
				else pathToCheck += $"/{parameter}";
				
				bool isDirectoryExists = await CheckDirectory(pathToCheck);
				if (!isDirectoryExists)
				{
					Console.WriteLine("Directory doesn't exist!");
					return;
				}
			}

			WebHDFSClient webHDFSClient = new WebHDFSClient(URL);
			FileStatuses fileStatuses = await webHDFSClient.ListStatus(pathToCheck);

			string output = "Directory content:\n<File Name>\t-\t<File Type>\n\n";

			foreach (FileStatus file in fileStatuses.FileStatus) output += $"{file.pathSuffix}\t\t-\t{file.type}\n";
			
			Console.WriteLine(output);
		}
	}
}

/*
 using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net.Http;

namespace HaboopaConsole
{
  internal class Program
  {
	private static string PATH = "/";

	private static string USERNAME = "";
	private static string HOST = "";
	private static string PORT = "";
	private static string URL = "";

	private static HttpClient client;

	static async Task Main(string[] args)
	{
	  client = new HttpClient();
	  if (args.Length == 0) return;
	  // extract args
	  HOST = args[0];
	  PORT = args[1];
	  USERNAME = args[2];

	  URL = $"http://{HOST}:{PORT}/webhdfs/v1/user/{USERNAME}";

	  string command = RequestCommand();
	  while (command != "exit")
	  {
		string[] extractedCommand = command.Split(' ');
		Console.WriteLine();

		switch (extractedCommand[0])
		{
		  case "cd":
			CD(extractedCommand[1]);
			break;
		  case "ls":
			await LS(); 
						break;
		}

		command = RequestCommand();
	  }
	}

	private static string RequestCommand()
	{
	  string template = $"{USERNAME}@{HOST}:{PATH} > ";
	  Console.Write(template);
	  
	  string command = Console.ReadLine();
	  if (command.StartsWith(template)) command = command.Substring(template.Length);

	  return command;
	}

	public static void CD(string newPath)
	{
	  if (string.IsNullOrEmpty(newPath)) return;
	  if (newPath == "..")
	  {
		string[] splittedPath = PATH.Split(new[] { "/" }, StringSplitOptions.RemoveEmptyEntries);
		Array.Resize(ref splittedPath, splittedPath.Length - 1);

		PATH = "/" + string.Join("/", splittedPath);
	  } else
	  {
		if (newPath.StartsWith("/") || PATH == "/") PATH = $"/{newPath}";
		else PATH += $"/{newPath}";
	  }
	  if (PATH.StartsWith("//")) PATH = PATH.Replace("//", "/");
	}

	public static async Task LS()
	{
	  // curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS"
	  string uri = "";
	  if (PATH != "/")
	  {
		uri = $"{URL}{PATH}?op=LISTSTATUS";
	  } else
	  {
		uri = $"{URL}?op=LISTSTATUS";
	  }

			await Console.Out.WriteLineAsync(uri);
	  using (var response = await client.GetAsync(uri))
	  {
		var content = await response.Content.ReadAsStringAsync();
				await Console.Out.WriteLineAsync(content);
			}
		}
  }
}
 */
