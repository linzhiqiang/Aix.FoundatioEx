set -ex

cd $(dirname $0)/../

artifactsFolder="./artifacts"

if [ -d $artifactsFolder ]; then
  rm -R $artifactsFolder
fi

mkdir -p $artifactsFolder

dotnet restore ./Aix.FoundatioEx.sln
dotnet build ./Aix.FoundatioEx.sln -c Release


dotnet pack ./src/Aix.FoundatioMessagingEx.Kafka/Aix.FoundatioMessagingEx.Kafka.csproj -c Release -o $artifactsFolder
