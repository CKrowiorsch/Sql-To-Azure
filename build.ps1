$currentPath = Get-Location
dotnet build -c Release -clp:nosummary -nologo -v:m -o "$currentPath\Build\Output\Client\" "Source\AzureSqlExporter\AzureSqlExporter.csproj" 