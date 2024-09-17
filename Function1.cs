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
    public class Function1
    {
        private readonly ILogger<Function1> _logger;

        public Function1(ILogger<Function1> logger)
        {
            _logger = logger;
        }

        [Function("Function1")]
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequest req)
        {
            string connectionString = "Provider=MSOLAP;Data Source=powerbi://api.powerbi.com/v1.0/myorg/DCDD%20Advogados%20Dashboard;User ID=app:c3da7a91-d044-4525-bdb4-2eb7c6af5aba@7a9e4f31-3a03-45c5-8f56-60c4e749c2ef;Password=tBm8Q~rFDsY1R2l6SlaJLoLOShRja4C3xFGqVbRt;Initial Catalog=DCDD Dashboard v2";
            string extractorAppName = "Azure Function C#";
            string extractorAppVersion = "1.0.0";
            bool excludeVpa = false;
            bool excludeTom = false;
            int columnBatchSize = 100; // Ajuste conforme necessário
            DirectQueryExtractionMode directQueryMode = DirectQueryExtractionMode.None;
            

            

            try
            {
                _logger.LogInformation("Iniciando a extração do modelo DAX...");

                // Obtém o DaxModel
                var daxModel = TomExtractor.GetDaxModel(
                    connectionString: connectionString,
                    applicationName: extractorAppName,
                    applicationVersion: extractorAppVersion,
                    readStatisticsFromData: true,
                    sampleRows: 0,
                    statsColumnBatchSize: columnBatchSize
                );

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
                Dax.ViewVpaExport.Model vpaModel = null;
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
                        var jsonResult = JsonConvert.SerializeObject(daxModel, new JsonSerializerSettings
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
