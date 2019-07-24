$currentPath = Get-Location
dotnet build -c Release -o "$currentPath\Build\Output\Client\" "Source\AzureSqlExporter\AzureSqlExporter.csproj" -nologo