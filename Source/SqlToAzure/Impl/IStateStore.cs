using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Krowiorsch.Model;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;

namespace Krowiorsch.Impl
{
    public interface IStateStore
    {
        Task<ImportState> ByIdentifier(string identifier);

        Task<string[]> AllStates();

        Task UpdateImportState(ImportState state);
    }

    class AzureBlobStateStore : IStateStore
    {
        readonly CloudBlobClient _client;
        readonly CloudBlobContainer _settingsContainer;

        public AzureBlobStateStore(string azureConnection)
        {
            var storageAccount = CloudStorageAccount.Parse(azureConnection);
            _client = storageAccount.CreateCloudBlobClient();
            _settingsContainer = _client.GetContainerReference("importstate");
        }

        public async Task<ImportState> ByIdentifier(string identifier)
        {
            if (!await _settingsContainer.ExistsAsync())
                return null;

            var blob = _settingsContainer.GetBlobReference(identifier);

            if (!await blob.ExistsAsync())
                return null;

            using (var stream = await blob.OpenReadAsync())
            using (var streamReader = new StreamReader(stream, Encoding.Default))
            {
                var jsonData = await streamReader.ReadLineAsync();
                return JsonConvert.DeserializeObject<ImportState>(jsonData);
            }
        }

        public async Task<string[]> AllStates()
        {
            if (!await _settingsContainer.ExistsAsync())
                return new string[0];

            var segment = await _settingsContainer.ListBlobsSegmentedAsync(null);

            var resultList = new List<IListBlobItem>();

            resultList.AddRange(segment.Results);

            while (segment.ContinuationToken != null)
            {
                segment = await _settingsContainer.ListBlobsSegmentedAsync(segment.ContinuationToken);
                resultList.AddRange(segment.Results);
            }

            return resultList.Cast<CloudBlockBlob>().Select(t => t.Name).ToArray();
        }

        public async Task UpdateImportState(ImportState state)
        {
            await _settingsContainer.CreateIfNotExistsAsync();

            var blobReference = _settingsContainer.GetBlockBlobReference(state.Identifier);
            await blobReference.UploadTextAsync(JsonConvert.SerializeObject(state));
        }
    }
}