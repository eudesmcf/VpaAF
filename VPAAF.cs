using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Dax.Metadata;
using Dax.Model.Extractor;
using Dax.Vpax;
using Dax.Vpax.Tools;
using System;
using System.IO;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace VpaAF
{
    public class ConnectionDetails
    {

        public string WorkspaceConnection { get; set; }
        public string AppId { get; set; }
        public string TenantId { get; set; }
        public string AppSecret { get; set; }
        public string Catalog { get; set; }
    }

    public class VPA
    {
        private readonly ILogger<VPA> _logger;

        public VPA(ILogger<VPA> logger)
        {
            _logger = logger;
        }

        [Function("VPA")]
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequest req)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            ConnectionDetails connDetails;

            try
            {
                connDetails = JsonConvert.DeserializeObject<ConnectionDetails>(requestBody);
            }
            catch (Exception ex)
            {
                _logger.LogError($"Erro ao desserializar os detalhes da conexão: {ex.Message}");
                return new ContentResult
                {
                    Content = "Invalid request body. Please send connection details in JSON format.",
                    ContentType = "text/plain",
                    StatusCode = StatusCodes.Status400BadRequest
                };
            }

            // Reconstrói a connectionString a partir dos dados recebidos
            string connectionString = $"Provider=MSOLAP;" +
                          $"Data Source={connDetails.WorkspaceConnection};" +
                          $"User ID=app:{connDetails.AppId + "@" + connDetails.TenantId};" +
                          $"Password={connDetails.AppSecret};" +
                          $"Initial Catalog={connDetails.Catalog}";

            string extractorAppName = "Azure Function C#";
            string extractorAppVersion = "1.0.0";
            bool excludeVpa = false;
            bool excludeTom = false;
            int columnBatchSize = 1000; // Ajuste conforme necessário

            try
            {
                _logger.LogInformation("Iniciando a extração do modelo DAX...");

                // Obtém o DaxModel
                var daxModel = await Task.Run(() => TomExtractor.GetDaxModel(
                    connectionString: connectionString,
                    applicationName: extractorAppName,
                    applicationVersion: extractorAppVersion,
                    readStatisticsFromData: true,
                    sampleRows: 10,
                    statsColumnBatchSize: columnBatchSize
                ));



                if (daxModel == null)
                {
                    _logger.LogError("O modelo DAX não pôde ser obtido. Verifique a string de conexão e as configurações.");
                    return new ContentResult
                    {
                        Content = "DAX model could not be retrieved.",
                        ContentType = "text/plain",
                        StatusCode = StatusCodes.Status500InternalServerError
                    };
                }

                _logger.LogInformation("Modelo DAX obtido com sucesso.");

                // Cria o modelo VPA se não estiver excluído
                Dax.ViewVpaExport.Model? vpaModel = null;
                if (!excludeVpa)
                {
                    try
                    {
                        vpaModel = new Dax.ViewVpaExport.Model(daxModel);
                        _logger.LogInformation("Modelo VPA criado com sucesso.");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"Erro ao criar o modelo VPA: {ex.Message}");
                        return new ContentResult
                        {
                            Content = "Error creating VPA model.",
                            ContentType = "text/plain",
                            StatusCode = StatusCodes.Status500InternalServerError
                        };
                    }
                }

                // Obtém o banco de dados TOM se não estiver excluído
                var tomDatabase = excludeTom ? null : TomExtractor.GetDatabase(connectionString);

                // Exporta o VPAX
                using (var vpaxStream = new MemoryStream())
                {
                    try
                    {
                        VpaxTools.ExportVpax(vpaxStream, daxModel, vpaModel, tomDatabase);
                        _logger.LogInformation("Exportação VPAX concluída com sucesso.");



                        // Serializa o resultado para JSON com configurações para ignorar referências circulares
                        var jsonResult = JsonConvert.SerializeObject(vpaModel, new JsonSerializerSettings
                        {
                            Formatting = Formatting.Indented,
                            ReferenceLoopHandling = ReferenceLoopHandling.Ignore
                        });

                        return new ContentResult
                        {
                            Content = jsonResult,
                            ContentType = "application/json",
                            StatusCode = StatusCodes.Status200OK
                        };
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"Erro ao exportar VPAX: {ex.Message}");
                        return new ContentResult
                        {
                            Content = "Error exporting VPAX.",
                            ContentType = "text/plain",
                            StatusCode = StatusCodes.Status500InternalServerError
                        };
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Erro inesperado: {ex.Message}");
                return new ContentResult
                {
                    Content = "Unexpected error.",
                    ContentType = "text/plain",
                    StatusCode = StatusCodes.Status500InternalServerError
                };
            }
        }
    }
}
