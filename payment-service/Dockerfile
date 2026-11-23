FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY payment-service/PaymentService.csproj ./payment-service/
RUN dotnet restore payment-service/PaymentService.csproj
COPY payment-service/ ./payment-service/
RUN dotnet publish payment-service/PaymentService.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0
ARG APP_VERSION=v1
ARG DEFAULT_LATENCY_MS=0
ARG DEFAULT_FRAUD_STRICT=false
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app/publish ./
ENV APP_VERSION=${APP_VERSION} \
    ARTIFICIAL_LATENCY_MS=${DEFAULT_LATENCY_MS} \
    FRAUD_CHECK_STRICT=${DEFAULT_FRAUD_STRICT} \
    REST_PORT=8080 \
    GRPC_PORT=9090 \
    DOTNET_EnableDiagnostics=0
USER 1654:1654
EXPOSE 8080 9090
HEALTHCHECK --interval=5s --timeout=3s --retries=12 CMD curl -fsS http://localhost:8080/health || exit 1
ENTRYPOINT ["dotnet", "PaymentService.dll"]
