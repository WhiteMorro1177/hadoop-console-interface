using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using System.Net;
using WebHDFS;
using System.Web.UI.WebControls;
using System.IO;

/*

Разработать клиент HDFS, поддерживающий операции:
− mkdir <имя каталога в HDFS> (создание каталога в HDFS);
− put <имя локального файла> (загрузка файла в HDFS);
− get <имя файла в HDFS> (скачивание файла из HDFS);
− append <имя локального файла> <имя файла в HDFS> (конкатенация файла в HDFS с локальным файлом);
− delete <имя файла в HDFS> (удаление файла в HDFS);
− ls (отображение содержимого текущего каталога в HDFS с разделением файлов и каталогов);
− cd <имя каталога в HDFS> (переход в другой каталог в HDFS, ".." — на уровень выше);
− lls (отображение содержимого текущего локального каталога с разделением файлов и каталогов);
− lcd <имя локального каталога> (переход в другой локальный каталог, ".." — на уровень выше).

Имена файлов и каталогов не содержат путей и символа "/".
Параметры командной строки клиента: сервер, порт, имя пользователя.
Пример запуска клиента: ./myhdfscli.rb localhost 50070 aslebedev
Использовать WebHDFS REST API и любой язык программирования.

*/

namespace HaboopaConsole
{
	internal class Program
	{
		// private static string LOCAL_PATH = Directory.GetCurrentDirectory();
		private static string PATH = "/";

		private static string USERNAME = "";
		private static string HOST = "";
		private static string PORT = "";
		private static string URL = "";

		private static HttpClient httpClient;
		private static WebHDFSClient webHDFSClient;

		static async Task Main(string[] args)
		{
			httpClient = new HttpClient();
			if (args.Length == 0) return;
			// extract args
			HOST = args[0];
			PORT = args[1];
			USERNAME = args[2];

			URL = $"http://{HOST}:{PORT}/webhdfs/v1/user/{USERNAME}";

			webHDFSClient = new WebHDFSClient(URL);

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
					case "mkdir":
						await MKDIR(extractedCommand[1]);
						break;
					case "delete":
						if (extractedCommand.Length == 3)
						{
							if (extractedCommand[1] == "-r")
							{
								await DELETE(extractedCommand[2], true);
							}
						} else await DELETE(extractedCommand[1]);
						break;
					case "lls":
						LLS();
						break;
					case "lcd":
						LCD(extractedCommand[1]);
						break;
				}

				command = RequestCommand();
			}
		}

		// support methods
		private static string RequestCommand()
		{
			string template = $"{USERNAME}@{HOST}:{PATH} > ";
			Console.Write(template);

			string command = Console.ReadLine();
			if (command.StartsWith(template)) command = command.Substring(template.Length);

			return command;
		}

		private static string PathConvert(string parameter)
		{
			string pathToDelete = PATH;

			if (parameter.StartsWith("/")) pathToDelete = parameter;
			else
			{
				pathToDelete += $"/{parameter}";
				pathToDelete = pathToDelete.Replace("//", "/");
			}

			return pathToDelete;
		}


		// main server methods
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
			if (!isDirectoryExists)
			{
				Console.WriteLine("No such file or directory!");
				PATH = pathBuffer;
			}
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

			FileStatuses fileStatuses = await webHDFSClient.ListStatus(pathToCheck);

			string output = "Directory content:\n<File Name>\t-\t<File Type>\n\n";

			foreach (FileStatus file in fileStatuses.FileStatus) output += $"{file.pathSuffix}\t\t-\t{file.type}\n";

			Console.WriteLine(output);
		}

		public static async Task MKDIR(string parameter)
		{
			string pathToMake = PathConvert(parameter);

			bool isDirectoryExists = await CheckDirectory(pathToMake);
			if (isDirectoryExists)
			{
				Console.WriteLine("Directory already exist!");
				return;
			}
            
			WebHDFS.Boolean isDirectoryCreated = await webHDFSClient.MakeDirectory(pathToMake);

			if (isDirectoryCreated.boolean) Console.WriteLine("Directory created!");
			else Console.WriteLine("Something went wrong");
		}

		public static async Task DELETE(string path, bool recursive = false)
		{
			string pathToDelete = PathConvert(path);

			bool isDirectoryExists = await CheckDirectory(pathToDelete);
			if (!isDirectoryExists)
			{
				Console.WriteLine("Directory doesn't exist!");
				return;
			}

			WebHDFS.Boolean isDeleted = await webHDFSClient.Delete(pathToDelete, recursive);

			if (isDeleted.boolean) Console.WriteLine("Object deleted!");
			else Console.WriteLine("Something went wrong");
		}


		// main local methods
		public static void LCD(string path)
		{
			path = path.Replace("/", "\\");
			if (path == "..")
			{
				string localPath = Directory.GetCurrentDirectory();
				string newPath = Directory.GetParent(localPath).FullName;
				Directory.SetCurrentDirectory(newPath);
				Console.WriteLine($"Work directory path changed to {newPath}");
			}
			else
			{
				try
				{
					string newPath = string.Join("\\", Directory.GetCurrentDirectory(), path);
					Directory.SetCurrentDirectory(newPath);
					Console.WriteLine($"Work directory path changed to {newPath}");
				}
				catch (DirectoryNotFoundException exc)
				{
					Console.WriteLine("No such directory!");
				}
				catch (Exception exc)
				{
					Console.WriteLine("Unhelded exception: " + exc);
				}

			}
		}

		public static void LLS(string path = null)
		{
			string output = "Directory Content\t<File Name>\t-\t<File Type>\n\n";
            if (path == null)
			{
				string[] files = Directory.GetFiles(Directory.GetCurrentDirectory());
				foreach (string file in files) output += $"{file}\t-\tFILE\n";

				string[] directories = Directory.GetDirectories(Directory.GetCurrentDirectory());
				foreach (string directory in directories) output += $"{directory}\t-\tDIRECTORY\n";
			}
            Console.WriteLine(output);
		}
	}
}
