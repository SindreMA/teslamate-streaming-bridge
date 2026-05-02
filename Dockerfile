FROM mcr.microsoft.com/dotnet/sdk:10.0-alpine AS build
WORKDIR /src
COPY TeslamateStreamingBridge.csproj ./
RUN dotnet restore TeslamateStreamingBridge.csproj
COPY . .
RUN dotnet publish TeslamateStreamingBridge.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0-alpine AS runtime
WORKDIR /app
COPY --from=build /app ./
ENV ASPNETCORE_URLS=http://+:8081
EXPOSE 8081
USER 1000
ENTRYPOINT ["dotnet", "TeslamateStreamingBridge.dll"]
