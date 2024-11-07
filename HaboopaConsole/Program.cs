using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using System.Net;
using WebHDFS;
using System.Web.UI.WebControls;
using System.IO;
using System.Web.UI.WebControls.WebParts;
using System.Diagnostics;

/*

Разработать клиент HDFS, поддерживающий операции:
+ mkdir <имя каталога в HDFS> (создание каталога в HDFS);
+ put <имя локального файла> (загрузка файла в HDFS);
− get <имя файла в HDFS> (скачивание файла из HDFS);
− append <имя локального файла> <имя файла в HDFS> (конкатенация файла в HDFS с локальным файлом);
+ delete <имя файла в HDFS> (удаление файла в HDFS);
+ ls (отображение содержимого текущего каталога в HDFS с разделением файлов и каталогов);
+ cd <имя каталога в HDFS> (переход в другой каталог в HDFS, ".." — на уровень выше);
+ lls (отображение содержимого текущего локального каталога с разделением файлов и каталогов);
+ lcd <имя локального каталога> (переход в другой локальный каталог, ".." — на уровень выше).

Имена файлов и каталогов не содержат путей и символа "/".
Параметры командной строки клиента: сервер, порт, имя пользователя.
Пример запуска клиента: ./myhdfscli.rb localhost 50070 aslebedev
Использовать WebHDFS REST API и любой язык программирования.

*/

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
		private static WebHDFSClient webHDFSClient;

		static async Task Main(string[] args)
		{
			httpClient = new HttpClient(new HttpClientHandler() { AllowAutoRedirect = false });
			if (args.Length == 0) return;
			// extract args
			HOST = args[0];
			PORT = args[1];
			USERNAME = args[2];

			URL = $"http://{HOST}:{PORT}/webhdfs/v1/user/{USERNAME}";

			webHDFSClient = new WebHDFSClient(URL, new NetworkCredential() { UserName = USERNAME });

			string command = RequestCommand();
			while (command != "exit")
			{
				List<string> extractedCommand = new List<string>();
				//string[] extractedCommand = command.Split(' ');
				if (command.Contains("\""))
				{
					// filename contains spaces
					string[] filenameExtraction = command.Split(new[] { "\"" }, StringSplitOptions.RemoveEmptyEntries);
					foreach (string part in filenameExtraction[0].Split(new[] { " " }, StringSplitOptions.RemoveEmptyEntries))
					{
						extractedCommand.Add(part);
					}
					extractedCommand.AddRange(filenameExtraction);
				}
				else extractedCommand = command.Split(' ').ToList();

				Console.WriteLine();

				switch (extractedCommand[0])
				{
					case "cd":
						await CD(extractedCommand[1]);
						break;
					case "ls":
						Dictionary<string, string> lsResult;
						if (extractedCommand.Count > 1) lsResult = await LS(extractedCommand[1]);
						else lsResult = await LS();

						if (lsResult == null) Console.WriteLine("Directory does not exist!");
						else
						{
							Console.WriteLine(lsResult["header"]);
							// foreach (var record in lsResult) Console.WriteLine($"{record.Key}\t\t\t\t{record.Value}");
							for (int i = 1; i < lsResult.Count; i++)
							{
								var currentKey = lsResult.Keys.ToList()[i];
								Console.WriteLine($"{currentKey}\t\t{lsResult[currentKey]}");
							}
						}
						break;
					case "mkdir":
						await MKDIR(extractedCommand[1]);
						break;
					case "delete":
						if (extractedCommand.Count == 3)
						{
							if (extractedCommand[1] == "-r")
							{
								await DELETE(extractedCommand[2], true);
							}
							else Console.WriteLine($"Flag '{extractedCommand[1]}' not found in this command");
						}
						else await DELETE(extractedCommand[1]);
						break;
					case "put":
						if (extractedCommand.Count == 3)
						{
							if (extractedCommand[1] == "--no-overwrite")
							{
								await PUT(extractedCommand[2], false);
							}
							else Console.WriteLine($"Flag '{extractedCommand[1]}' not found in this command");
						}
						else await PUT(extractedCommand[1]);
						break;
					case "get":
						await GET(extractedCommand[1]);
						break;
					case "append":
						await APPEND(extractedCommand[1], extractedCommand[2]);
						break;
					case "lls":
						LLS();
						break;
					case "lcd":
						if (extractedCommand.Count <= 1) break;
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
			string convertedPath = PATH;

			if (parameter.StartsWith("/")) convertedPath = parameter;
			else
			{
				convertedPath += $"/{parameter}".Replace("//", "/");
			}

			return convertedPath;
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

		public static async Task<Dictionary<string, string>> LS(string parameter = null)
		{
			// convert path and check directory
			string pathToCheck = "";
			if (parameter == null) pathToCheck = PATH;
			else
			{
				if (parameter.StartsWith("/")) pathToCheck = parameter;
				else pathToCheck += $"/{parameter}";

				bool isDirectoryExists = await CheckDirectory(pathToCheck);
				if (!isDirectoryExists) return null;

			}

			Dictionary<string, string> result = new Dictionary<string, string>()
			{
				{ "header", "Directory content:\n<File Name>\t\t<File Type>\n" }
			};

			FileStatuses fileStatuses = await webHDFSClient.ListStatus(pathToCheck);
			// string output = "Directory content:\n<File Name>\t\t\t\t<File Type>\n\n";

			foreach (FileStatus file in fileStatuses.FileStatus) result.Add(file.pathSuffix, file.type); //output += $"{file.pathSuffix}\t\t\t\t\t{file.type}\n";

			return result;
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

			WebHDFS.Boolean isDirectoryCreated = await webHDFSClient.MakeDirectory(pathToMake.Replace("//", "/"));

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

		public static async Task PUT(string filename, bool isOverwrite = true)
		{
			string[] existedFiles = Directory.GetFiles(Directory.GetCurrentDirectory());

			if (existedFiles.Contains(Path.GetFullPath(filename)))
			{
				string path = (PATH == "/") ? "" : PATH;
				var response = await httpClient.PutAsync($"{URL}{path}/{filename}?op=CREATE&overwrite={isOverwrite}", null);
				if (response.StatusCode == HttpStatusCode.TemporaryRedirect)
				{
					using (Stream fileStream = File.OpenRead(filename))
					{
						Uri newURL = response.Headers.Location;
						response = await httpClient.PutAsync(newURL, new StreamContent(fileStream));
						if (response.StatusCode == HttpStatusCode.Created)
						{
							Console.WriteLine($"File \'{Path.GetFileName(filename)}\' was uploaded in \'{PATH}\'");
						}
						else
						{
							Console.WriteLine($"Something went wrong");
						}
					}
				}
			}
		}

		public static async Task GET(string filename, bool isOverwriteLocalfile = false)
		{
			HttpContent fileContent = null;
			Dictionary<string, string> existedFiles = await LS();

			if (!existedFiles.ContainsKey(filename))
			{
				Console.WriteLine("File does not exist!");
				return;
			}

			string path = (PATH == "/") ? "" : PATH;
			var response = await httpClient.GetAsync($"{URL}{path}/{filename}?op=OPEN");
			if (response.StatusCode == HttpStatusCode.TemporaryRedirect)
			{
				Uri newURL = response.Headers.Location;
				response = await httpClient.GetAsync(newURL);

				if (response.StatusCode == HttpStatusCode.OK)
				{
					fileContent = response.Content;
				}
				else Console.WriteLine("Something went wrong");
			}
			else
			{
				fileContent = null; 
				Console.WriteLine("Something went wrong");
			}

			if (fileContent == null) return;

			// check local file
			string[] existedLocalFiles = Directory.GetFiles(Directory.GetCurrentDirectory());
			string localFilePath = Path.Combine(Directory.GetCurrentDirectory(), filename);

			if (existedLocalFiles.Contains(localFilePath))
			{
				File.Delete(localFilePath);	
			}

			byte[] fileContentAsBytes = await fileContent.ReadAsByteArrayAsync();
			File.WriteAllBytes(localFilePath, fileContentAsBytes);
		}

		public static async Task APPEND(string localFileName, string hdfsFileName)
		{
			string[] existedLocalFiles = Directory.GetFiles(Directory.GetCurrentDirectory());
			Dictionary<string, string> existedHdfsFiles = await LS();

			if (!existedLocalFiles.Contains(Path.GetFullPath(localFileName)))
			{
                Console.WriteLine("Local file does not exist");
				return;
			}

			if (!existedHdfsFiles.ContainsKey(hdfsFileName))
			{
				Console.WriteLine("HDFS File does not exist!");
				return;
			}

			string path = (PATH == "/") ? "" : PATH;
			var response = await httpClient.PostAsync($"{URL}{path}/{hdfsFileName}?op=APPEND", null);

			if (response.StatusCode == HttpStatusCode.TemporaryRedirect)
			{
				using (Stream fileStream = File.OpenRead(localFileName))
				{
					Uri newURL = response.Headers.Location;
					response = await httpClient.PostAsync(newURL, new StreamContent(fileStream));
					if (response.StatusCode == HttpStatusCode.OK)
					{
						Console.WriteLine($"File \'{Path.GetFileName(localFileName)}\' was appended to \'{hdfsFileName}\'");
					}
					else
					{
						Console.WriteLine($"Something went wrong - Status: {response.StatusCode}");
					}
				}
			}

			//var response = await httpClient.PostAsync($"{URL}{path}/{filename}?op=APPEND", null);
			//if (response.StatusCode == HttpStatusCode.TemporaryRedirect)
			//{
			//	using (Stream fileStream = File.OpenRead(filename))
			//	{
			//		Uri newURL = response.Headers.Location;
			//		response = await httpClient.PutAsync(newURL, new StreamContent(fileStream));
			//		if (response.StatusCode == HttpStatusCode.Created)
			//		{
			//			Console.WriteLine($"File \'{Path.GetFileName(filename)}\' was uploaded in \'{PATH}\'");
			//		}
			//		else
			//		{
			//			Console.WriteLine($"Something went wrong");
			//		}
			//	}
			//}

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
