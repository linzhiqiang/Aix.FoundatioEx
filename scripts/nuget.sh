set -ex

cd $(dirname $0)/../

artifactsFolder="./artifacts"

if [ -d $artifactsFolder ]; then
  rm -R $artifactsFolder
fi

mkdir -p $artifactsFolder


dotnet build ./src/Aix.FoundatioEx.Kafka/Aix.FoundatioEx.Kafka.csproj -c Release

dotnet pack ./src/Aix.FoundatioEx.Kafka/Aix.FoundatioEx.Kafka.csproj -c Release -o ../../$artifactsFolder

dotnet nuget push ./$artifactsFolder/Aix.FoundatioEx.Kafka.*.nupkg -k $LOCAL_NUGET_KEY -s http://localhost:49544/nuget
