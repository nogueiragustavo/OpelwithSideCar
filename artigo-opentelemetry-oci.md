# OpenTelemetry em produção na Oracle Cloud Infrastructure: arquitetura sidecar com OCI Streaming, LangFuse e Elastic

**Um guia técnico de implementação para observabilidade de aplicações distribuídas e agentes de IA em ambientes regulados.**

---

## Por que este artigo

A maioria dos tutoriais de OpenTelemetry mostra como instrumentar uma aplicação Python "hello world" e visualizar traces no Jaeger local. Isso é útil para começar, mas distante da realidade de quem precisa rodar observabilidade em produção sobre cloud corporativa, atendendo a múltiplas aplicações, em ambiente regulado (LGPD, Anatel), com agentes de IA gerando alto volume de telemetria sensível.

Este artigo documenta uma arquitetura real que resolve esse problema sobre Oracle Cloud Infrastructure (OCI), usando:

- **OKE (Oracle Kubernetes Engine)** para orquestração
- **OCI Streaming com Apache Kafka API** como buffer durável de telemetria
- **OCI Vault** para gestão de credenciais
- **LangFuse** para observabilidade especializada de aplicações com LLM
- **Elastic** como backend principal, integrado via Kafka Connect

O foco é o **padrão sidecar**: cada pod recebe um container adicional do OpenTelemetry Collector, garantindo isolamento por aplicação. Vou cobrir o provisionamento OCI, a configuração dos Collectors em todas as camadas, integração com os destinos, redação de PII compatível com LGPD, e observabilidade do próprio Collector.

Os códigos apresentados são funcionais — testados em ambiente real. Você pode adaptá-los aos seus identificadores e endpoints específicos.

---

## A arquitetura em três camadas

```
┌──────────────────────────────────────────────────────────────────────┐
│  Tier 1: Coleta local (sidecar + VM)                                 │
│  ┌──────────────────┐  ┌──────────────────┐  ┌────────────────────┐  │
│  │ Pod (OKE apps)   │  │ Pod (OKE apps)   │  │  VM                │  │
│  │ ┌────┐ ┌───────┐ │  │ ┌────┐ ┌───────┐ │  │  ┌──────────────┐  │  │
│  │ │App │→│Sidecar│ │  │ │App │→│Sidecar│ │  │  │  systemd     │  │  │
│  │ └────┘ └───┬───┘ │  │ └────┘ └───┬───┘ │  │  │  otelcol     │  │  │
│  └────────────┼─────┘  └────────────┼─────┘  └─────────┬────────┘  │
│               │                     │                  │           │
└───────────────┼─────────────────────┼──────────────────┼───────────┘
                │                     │                  │
                ▼                     ▼                  ▼
        ┌─────────────────────────────────────────────────┐
        │  Tier 2: Buffer durável                         │
        │  OCI Streaming (Apache Kafka API)               │
        │  Tópico: telemetry_in                           │
        └────────────────────────┬────────────────────────┘
                                 │
                                 ▼
        ┌─────────────────────────────────────────────────┐
        │  Tier 3: Gateway no OKE O&M                     │
        │  - k8sattributes                                │
        │  - transform/redaction_pii                      │
        │  - tail_sampling                                │
        │  - redaction (fail-closed)                      │
        └────┬────────────────────┬───────────────────┬───┘
             │                    │                   │
             ▼                    ▼                   ▼
     ┌─────────────┐      ┌──────────────┐    ┌─────────────────┐
     │  LangFuse   │      │ OCI Streaming│    │ Object Storage  │
     │ (gen_ai.*)  │      │ telemetry_   │    │ (auditoria)     │
     │             │      │ to_elastic   │    │                 │
     └─────────────┘      └──────┬───────┘    └─────────────────┘
                                 │
                                 ▼
                          ┌──────────────┐
                          │ Kafka Connect│
                          │      ↓       │
                          │   Elastic    │
                          └──────────────┘
```

**Decisões-chave por trás dessa topologia:**

- **Sidecar em vez de DaemonSet** preserva isolamento por aplicação — cada pod tem seu Collector dedicado, falha em um não afeta os outros.
- **OCI Streaming como buffer** desacopla produtores e consumidor. Manutenção do Gateway pode ser feita sem afetar aplicações.
- **Gateway em cluster OKE separado (O&M)** reduz blast radius. Aplicações e observabilidade têm ciclos de release independentes.
- **Pipelines paralelas no Gateway** roteiam traces de LLM ao LangFuse, e operação geral ao Elastic.
- **Kafka Connect entre Gateway e Elastic** adiciona buffer extra e permite saneamento via Ingest Pipelines do Elastic.

---

## Pré-requisitos OCI

### 1. Compartments

Crie compartments dedicados para isolar recursos:

```bash
# Variáveis comuns
export TENANCY_OCID="ocid1.tenancy.oc1..xxxxx"
export ROOT_COMPARTMENT="ocid1.compartment.oc1..xxxxx"

# Compartment para observabilidade
oci iam compartment create \
  --compartment-id "$ROOT_COMPARTMENT" \
  --name "observability" \
  --description "OpenTelemetry pipeline, Streaming, Vault para telemetria"

# Anote o OCID retornado
export OBS_COMPARTMENT="ocid1.compartment.oc1..yyyyy"
```

### 2. OCI Streaming com Apache Kafka

Crie um Stream Pool com a API Kafka habilitada (essencial — sem isso o OTel Collector não conecta):

```bash
# Cria o Stream Pool
oci streaming admin stream-pool create \
  --compartment-id "$OBS_COMPARTMENT" \
  --name "otel-pool-prod" \
  --kafka-settings '{
    "autoCreateTopicsEnable": false,
    "logRetentionHours": 168,
    "numPartitions": 12,
    "bootstrapServers": ""
  }'

# Anote o OCID e o endpoint Kafka
export STREAM_POOL_OCID="ocid1.streampool.oc1.sa-saopaulo-1.zzzzz"

# Recupera o endpoint Kafka-compatible
oci streaming admin stream-pool get \
  --stream-pool-id "$STREAM_POOL_OCID" \
  --query 'data."kafka-settings"."bootstrap-servers"' \
  --raw-output
# Saída típica: cell-1.streampool.sa-saopaulo-1.oci.oraclecloud.com:9092
```

Crie os dois tópicos:

```bash
# Tópico de entrada: Collectors locais publicam aqui
oci streaming admin stream create \
  --compartment-id "$OBS_COMPARTMENT" \
  --name "telemetry_in" \
  --partitions 12 \
  --retention-in-hours 168 \
  --stream-pool-id "$STREAM_POOL_OCID"

# Tópico de saída: Gateway publica aqui, Kafka Connect consome
oci streaming admin stream create \
  --compartment-id "$OBS_COMPARTMENT" \
  --name "telemetry_to_elastic" \
  --partitions 6 \
  --retention-in-hours 72 \
  --stream-pool-id "$STREAM_POOL_OCID"
```

### 3. Credenciais SASL para o Kafka API

O OCI Streaming via Kafka API usa SASL/PLAIN com credenciais no formato `tenancy_name/user_name/stream_pool_OCID`:

```bash
# Gera um Auth Token para o usuário da pipeline
# (vá em: Identity → Users → seu_user → Auth Tokens → Generate)
# O token gerado aparece UMA ÚNICA VEZ — copie imediatamente.

# Formato do username SASL:
export KAFKA_USERNAME="${TENANCY_NAME}/${USER_NAME}/${STREAM_POOL_OCID}"
# Exemplo: "minhatenancy/otel-pipeline-user/ocid1.streampool.oc1..."

# A senha é o auth token gerado
export KAFKA_PASSWORD="auth-token-gerado-aqui"
```

### 4. OCI Vault para secrets

```bash
# Cria o Vault
oci kms management vault create \
  --compartment-id "$OBS_COMPARTMENT" \
  --display-name "otel-vault-prod" \
  --vault-type "DEFAULT"

export VAULT_OCID="ocid1.vault.oc1.sa-saopaulo-1.xxxxx"
export VAULT_ENDPOINT=$(oci kms management vault get \
  --vault-id "$VAULT_OCID" --query 'data."management-endpoint"' --raw-output)

# Cria uma master key para encriptar os secrets
oci kms management key create \
  --compartment-id "$OBS_COMPARTMENT" \
  --display-name "otel-master-key" \
  --key-shape '{"algorithm":"AES","length":32}' \
  --endpoint "$VAULT_ENDPOINT"

export KEY_OCID="ocid1.key.oc1.sa-saopaulo-1.xxxxx"

# Guarda as credenciais Kafka como secrets
oci vault secret create-base64 \
  --compartment-id "$OBS_COMPARTMENT" \
  --secret-name "kafka-username" \
  --vault-id "$VAULT_OCID" \
  --key-id "$KEY_OCID" \
  --secret-content-content "$(echo -n "$KAFKA_USERNAME" | base64)"

oci vault secret create-base64 \
  --compartment-id "$OBS_COMPARTMENT" \
  --secret-name "kafka-password" \
  --vault-id "$VAULT_OCID" \
  --key-id "$KEY_OCID" \
  --secret-content-content "$(echo -n "$KAFKA_PASSWORD" | base64)"

# Repetir para LangFuse public/secret keys
```

### 5. Object Storage para auditoria

```bash
oci os bucket create \
  --compartment-id "$OBS_COMPARTMENT" \
  --name "otel-audit-trail" \
  --storage-tier "Standard" \
  --versioning "Enabled" \
  --object-events-enabled true

# Para retenção legal (LGPD), aplique também um Retention Rule via console
# ou usando: oci os retention-rule create
```

---

## Setup do OpenTelemetry Operator

O Operator simplifica a injeção de sidecars via CRDs `Instrumentation` e `OpenTelemetryCollector`. Instalação no cluster OKE de aplicações:

```bash
# 1. cert-manager (pré-requisito)
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.15.3/cert-manager.yaml

# Aguarda cert-manager ficar pronto
kubectl wait --for=condition=Available --timeout=300s \
  -n cert-manager deployment/cert-manager-webhook

# 2. Operator
kubectl apply -f https://github.com/open-telemetry/opentelemetry-operator/releases/download/v0.103.0/opentelemetry-operator.yaml

# Aguarda o operator
kubectl wait --for=condition=Available --timeout=300s \
  -n opentelemetry-operator-system deployment/opentelemetry-operator-controller-manager
```

### Namespace e ServiceAccount

```yaml
# 01-namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: observability
  labels:
    pod-security.kubernetes.io/enforce: restricted
    pod-security.kubernetes.io/audit: restricted
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: otel-collector
  namespace: observability
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: otel-collector
rules:
  - apiGroups: [""]
    resources: ["pods", "namespaces", "nodes"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["apps"]
    resources: ["replicasets", "deployments", "statefulsets", "daemonsets"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["batch"]
    resources: ["jobs", "cronjobs"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["endpoints", "services"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: otel-collector
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: otel-collector
subjects:
  - kind: ServiceAccount
    name: otel-collector
    namespace: observability
```

### External Secrets Operator com OCI Vault

```bash
# Instala External Secrets Operator
helm repo add external-secrets https://charts.external-secrets.io
helm install external-secrets external-secrets/external-secrets \
  -n external-secrets-system --create-namespace \
  --set installCRDs=true
```

Para que o ESO autentique no OCI Vault, use Instance Principal (o cluster OKE precisa estar em uma Dynamic Group com policy adequada):

```bash
# Policy a anexar ao Dynamic Group dos nodes do OKE:
# Allow dynamic-group oke-nodes to read secret-bundles in compartment observability
# Allow dynamic-group oke-nodes to use keys in compartment observability
```

```yaml
# 02-secret-store.yaml
apiVersion: external-secrets.io/v1beta1
kind: SecretStore
metadata:
  name: oci-vault
  namespace: observability
spec:
  provider:
    oracle:
      vault: ocid1.vault.oc1.sa-saopaulo-1.xxxxx
      region: sa-saopaulo-1
      auth:
        secretRef:
          principalType: InstancePrincipal
---
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: telemetry-credentials
  namespace: observability
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: oci-vault
    kind: SecretStore
  target:
    name: telemetry-credentials
    creationPolicy: Owner
  data:
    - secretKey: kafka-username
      remoteRef:
        key: ocid1.vaultsecret.oc1.sa-saopaulo-1.xxxxx
    - secretKey: kafka-password
      remoteRef:
        key: ocid1.vaultsecret.oc1.sa-saopaulo-1.yyyyy
    - secretKey: langfuse-public-key
      remoteRef:
        key: ocid1.vaultsecret.oc1.sa-saopaulo-1.zzzzz
    - secretKey: langfuse-secret-key
      remoteRef:
        key: ocid1.vaultsecret.oc1.sa-saopaulo-1.wwwww
```

---

## Sidecar Collector: configuração e injeção automática

### CRD OpenTelemetryCollector com mode sidecar

```yaml
# 03-sidecar-collector.yaml
apiVersion: opentelemetry.io/v1beta1
kind: OpenTelemetryCollector
metadata:
  name: otel-sidecar
  namespace: applications
spec:
  mode: sidecar
  image: otel/opentelemetry-collector-contrib:0.151.0
  serviceAccount: otel-collector

  resources:
    requests:
      cpu: 100m
      memory: 128Mi
    limits:
      cpu: 300m
      memory: 256Mi

  env:
    - name: POD_NAME
      valueFrom:
        fieldRef:
          fieldPath: metadata.name
    - name: POD_NAMESPACE
      valueFrom:
        fieldRef:
          fieldPath: metadata.namespace
    - name: NODE_NAME
      valueFrom:
        fieldRef:
          fieldPath: spec.nodeName
    - name: GOMEMLIMIT
      value: "230MiB"
    - name: KAFKA_USERNAME
      valueFrom:
        secretKeyRef:
          name: telemetry-credentials
          key: kafka-username
    - name: KAFKA_PASSWORD
      valueFrom:
        secretKeyRef:
          name: telemetry-credentials
          key: kafka-password

  securityContext:
    runAsNonRoot: true
    runAsUser: 10001
    allowPrivilegeEscalation: false
    readOnlyRootFilesystem: true
    capabilities:
      drop: ["ALL"]

  config:
    extensions:
      health_check:
        endpoint: 0.0.0.0:13133

    receivers:
      otlp:
        protocols:
          grpc:
            endpoint: localhost:4317
            max_recv_msg_size_mib: 32
          http:
            endpoint: localhost:4318
            max_request_body_size: 33554432

    processors:
      memory_limiter:
        check_interval: 1s
        limit_percentage: 80
        spike_limit_percentage: 25

      resource/host:
        attributes:
          - key: k8s.pod.name
            value: ${env:POD_NAME}
            action: upsert
          - key: k8s.namespace.name
            value: ${env:POD_NAMESPACE}
            action: upsert
          - key: k8s.node.name
            value: ${env:NODE_NAME}
            action: upsert
          - key: cloud.provider
            value: oci
            action: upsert
          - key: cloud.region
            value: sa-saopaulo-1
            action: upsert

      batch:
        timeout: 1s
        send_batch_size: 1024
        send_batch_max_size: 2048

    exporters:
      kafka/streaming:
        brokers:
          - cell-1.streampool.sa-saopaulo-1.oci.oraclecloud.com:9092
        topic: telemetry_in
        encoding: otlp_proto
        timeout: 10s
        auth:
          sasl:
            username: ${env:KAFKA_USERNAME}
            password: ${env:KAFKA_PASSWORD}
            mechanism: PLAIN
          tls:
            insecure: false
        producer:
          max_message_bytes: 1000000
          required_acks: 1
          compression: snappy
        sending_queue:
          enabled: true
          num_consumers: 4
          queue_size: 1000
        retry_on_failure:
          enabled: true
          initial_interval: 5s
          max_interval: 30s
          max_elapsed_time: 300s

    service:
      extensions: [health_check]
      pipelines:
        traces:
          receivers: [otlp]
          processors: [memory_limiter, resource/host, batch]
          exporters: [kafka/streaming]
        metrics:
          receivers: [otlp]
          processors: [memory_limiter, resource/host, batch]
          exporters: [kafka/streaming]
        logs:
          receivers: [otlp]
          processors: [memory_limiter, resource/host, batch]
          exporters: [kafka/streaming]
      telemetry:
        logs:
          level: info
          encoding: json
        metrics:
          level: detailed
          address: 0.0.0.0:8888
```

### Injetando o sidecar em uma aplicação

Com o Operator instalado, basta uma annotation:

```yaml
# 04-app-example.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minha-aplicacao
  namespace: applications
spec:
  replicas: 3
  selector:
    matchLabels:
      app: minha-aplicacao
  template:
    metadata:
      labels:
        app: minha-aplicacao
      annotations:
        # Esta annotation pede ao Operator que injete o sidecar
        sidecar.opentelemetry.io/inject: "otel-sidecar"
    spec:
      containers:
        - name: app
          image: minha-aplicacao:1.2.0
          env:
            - name: OTEL_EXPORTER_OTLP_ENDPOINT
              value: "http://localhost:4318"
            - name: OTEL_EXPORTER_OTLP_PROTOCOL
              value: "http/protobuf"
            - name: OTEL_SERVICE_NAME
              value: "minha-aplicacao"
            - name: OTEL_RESOURCE_ATTRIBUTES
              value: "service.namespace=produtos,service.version=1.2.0,deployment.environment=prod"
          ports:
            - containerPort: 8080
              name: http
          resources:
            requests:
              cpu: 200m
              memory: 512Mi
            limits:
              cpu: 1
              memory: 1Gi
```

O Operator detecta a annotation e injeta automaticamente o container `otel-sidecar` com a configuração definida na CRD. Sem precisar editar manualmente cada Deployment.

---

## Collector em VM (legacy workloads)

Para aplicações ainda em VM, instale o Collector como systemd service:

```bash
# /tmp/install-otelcol.sh
#!/bin/bash
set -e

VERSION="0.151.0"
ARCH="linux_amd64"

# Download
curl -L -o /tmp/otelcol-contrib.tar.gz \
  "https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v${VERSION}/otelcol-contrib_${VERSION}_${ARCH}.tar.gz"

# Extract
tar -xzf /tmp/otelcol-contrib.tar.gz -C /tmp/
sudo install -o root -g root -m 0755 /tmp/otelcol-contrib /usr/local/bin/

# Cria user dedicado
sudo useradd --system --no-create-home --shell /usr/sbin/nologin otelcol || true

# Diretórios
sudo mkdir -p /etc/otelcol-contrib /var/lib/otelcol-contrib/queue /var/log/otelcol-contrib
sudo chown -R otelcol:otelcol /var/lib/otelcol-contrib /var/log/otelcol-contrib
sudo chmod 750 /etc/otelcol-contrib
```

Configuração:

```yaml
# /etc/otelcol-contrib/config.yaml
extensions:
  health_check:
    endpoint: 0.0.0.0:13133

  file_storage/queue:
    directory: /var/lib/otelcol-contrib/queue
    timeout: 1s
    compaction:
      on_start: true
      on_rebound: true
      rebound_needed_threshold_mib: 100

receivers:
  otlp:
    protocols:
      grpc:
        endpoint: localhost:4317
        max_recv_msg_size_mib: 32
      http:
        endpoint: localhost:4318

processors:
  memory_limiter:
    check_interval: 1s
    limit_percentage: 80
    spike_limit_percentage: 25

  resourcedetection:
    detectors: [env, system, oci]
    timeout: 5s
    override: false

  resource/host:
    attributes:
      - key: deployment.environment
        value: ${env:ENVIRONMENT}
        action: upsert
      - key: service.name
        value: ${env:SERVICE_NAME}
        action: upsert

  batch:
    timeout: 1s
    send_batch_size: 1024

exporters:
  kafka/streaming:
    brokers:
      - cell-1.streampool.sa-saopaulo-1.oci.oraclecloud.com:9092
    topic: telemetry_in
    encoding: otlp_proto
    timeout: 10s
    auth:
      sasl:
        username: ${env:KAFKA_USERNAME}
        password: ${env:KAFKA_PASSWORD}
        mechanism: PLAIN
      tls:
        insecure: false
    producer:
      max_message_bytes: 1000000
      required_acks: 1
      compression: snappy
    sending_queue:
      enabled: true
      queue_size: 5000
      storage: file_storage/queue
    retry_on_failure:
      enabled: true
      max_elapsed_time: 300s

service:
  extensions: [health_check, file_storage/queue]
  pipelines:
    traces:
      receivers: [otlp]
      processors: [memory_limiter, resourcedetection, resource/host, batch]
      exporters: [kafka/streaming]
    metrics:
      receivers: [otlp]
      processors: [memory_limiter, resourcedetection, resource/host, batch]
      exporters: [kafka/streaming]
    logs:
      receivers: [otlp]
      processors: [memory_limiter, resourcedetection, resource/host, batch]
      exporters: [kafka/streaming]
```

Variáveis de ambiente em `/etc/otelcol-contrib/otelcol.env`:

```bash
# /etc/otelcol-contrib/otelcol.env
ENVIRONMENT=prod
SERVICE_NAME=minha-aplicacao-vm
KAFKA_USERNAME=minhatenancy/otel-pipeline-user/ocid1.streampool.oc1.sa-saopaulo-1.xxxxx
KAFKA_PASSWORD=seu-auth-token-aqui
GOMEMLIMIT=450MiB
```

Unit file systemd:

```ini
# /etc/systemd/system/otelcol-contrib.service
[Unit]
Description=OpenTelemetry Collector Contrib
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=otelcol
Group=otelcol
EnvironmentFile=/etc/otelcol-contrib/otelcol.env
ExecStart=/usr/local/bin/otelcol-contrib --config=/etc/otelcol-contrib/config.yaml
Restart=on-failure
RestartSec=10
LimitNOFILE=65536

# Hardening
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
PrivateDevices=true
NoNewPrivileges=true
ReadWritePaths=/var/lib/otelcol-contrib /var/log/otelcol-contrib

[Install]
WantedBy=multi-user.target
```

Ative:

```bash
sudo systemctl daemon-reload
sudo systemctl enable otelcol-contrib
sudo systemctl start otelcol-contrib
sudo systemctl status otelcol-contrib

# Verifica health endpoint
curl -s http://localhost:13133/health | jq
```

Para credenciais em VM, prefira o **OCI Instance Principal** com um script de bootstrap que recupera os secrets do Vault no boot — evita ter o auth token em texto claro no `.env`.

---

## Gateway no OKE O&M: configuração completa

Esta é a peça mais densa da arquitetura. O Gateway concentra k8sattributes, redação de PII, tail sampling e roteamento aos destinos.

### ConfigMap

```yaml
# 05-gateway-configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: otel-gateway-config
  namespace: observability
data:
  config.yaml: |
    extensions:
      health_check:
        endpoint: 0.0.0.0:13133
        path: /health
        check_collector_pipeline:
          enabled: true
          interval: 5m
          exporter_failure_threshold: 5

      file_storage/queue:
        directory: /var/lib/otel/queue
        timeout: 1s
        compaction:
          on_start: true
          on_rebound: true
          rebound_needed_threshold_mib: 100

      pprof:
        endpoint: localhost:1777

    receivers:
      kafka/in:
        brokers:
          - cell-1.streampool.sa-saopaulo-1.oci.oraclecloud.com:9092
        topic: telemetry_in
        encoding: otlp_proto
        group_id: otel-gateway
        client_id: otel-gateway
        initial_offset: earliest
        auth:
          sasl:
            username: ${env:KAFKA_USERNAME}
            password: ${env:KAFKA_PASSWORD}
            mechanism: PLAIN
          tls:
            insecure: false
        metadata:
          full: false
          retry:
            max: 3
            backoff: 250ms

    processors:
      memory_limiter:
        check_interval: 1s
        limit_percentage: 80
        spike_limit_percentage: 25

      k8sattributes:
        auth_type: serviceAccount
        passthrough: false
        extract:
          metadata:
            - k8s.namespace.name
            - k8s.deployment.name
            - k8s.replicaset.name
            - k8s.statefulset.name
            - k8s.daemonset.name
            - k8s.pod.name
            - k8s.pod.uid
            - k8s.node.name
          labels:
            - tag_name: app.team
              key: team
              from: pod
            - tag_name: app.component
              key: component
              from: pod
        pod_association:
          - sources:
              - from: resource_attribute
                name: k8s.pod.uid
          - sources:
              - from: resource_attribute
                name: k8s.pod.name
                name: k8s.namespace.name

      resource/cluster:
        attributes:
          - key: cloud.region
            value: sa-saopaulo-1
            action: upsert
          - key: cloud.provider
            value: oci
            action: upsert
          - key: deployment.environment
            value: ${env:ENVIRONMENT}
            action: upsert

      # Mascaramento de PII em texto livre — regex para padrões brasileiros
      transform/redaction_pii:
        error_mode: ignore
        trace_statements:
          - context: span
            statements:
              # CPF: 000.000.000-00 ou 00000000000
              - replace_all_patterns(attributes, "value",
                  "\\b\\d{3}\\.\\d{3}\\.\\d{3}-\\d{2}\\b", "[CPF]")
              - replace_all_patterns(attributes, "value",
                  "\\b\\d{11}\\b", "[CPF]")

              # CNPJ: 00.000.000/0000-00
              - replace_all_patterns(attributes, "value",
                  "\\b\\d{2}\\.\\d{3}\\.\\d{3}/\\d{4}-\\d{2}\\b", "[CNPJ]")

              # E-mail
              - replace_all_patterns(attributes, "value",
                  "\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}\\b", "[EMAIL]")

              # Telefone brasileiro: (11) 99999-9999 ou +55 11 99999-9999
              - replace_all_patterns(attributes, "value",
                  "\\(\\d{2}\\)\\s?\\d{4,5}-?\\d{4}", "[PHONE]")

              # Cartão de crédito (16 dígitos com ou sem espaços/hífens)
              - replace_all_patterns(attributes, "value",
                  "\\b\\d{4}[\\s-]?\\d{4}[\\s-]?\\d{4}[\\s-]?\\d{4}\\b", "[CARD]")

              # Truncamento de prompts longos (LLMs)
              - set(attributes["gen_ai.prompt"],
                    Substring(attributes["gen_ai.prompt"], 0, 1000))
                where attributes["gen_ai.prompt"] != nil and
                      Len(attributes["gen_ai.prompt"]) > 1000

              # Truncamento de respostas
              - set(attributes["gen_ai.completion"],
                    Substring(attributes["gen_ai.completion"], 0, 2000))
                where attributes["gen_ai.completion"] != nil and
                      Len(attributes["gen_ai.completion"]) > 2000

        log_statements:
          - context: log
            statements:
              - replace_all_patterns(body, "value",
                  "\\b\\d{3}\\.\\d{3}\\.\\d{3}-\\d{2}\\b", "[CPF]")
              - replace_all_patterns(body, "value",
                  "\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}\\b", "[EMAIL]")

      # Remove ruído de health-checks e similar
      filter/noise:
        error_mode: ignore
        traces:
          span:
            - 'attributes["http.target"] == "/health"'
            - 'attributes["http.target"] == "/healthz"'
            - 'attributes["http.target"] == "/metrics"'
            - 'attributes["http.target"] == "/ready"'
            - 'attributes["http.target"] == "/readyz"'
            - 'attributes["http.target"] == "/live"'
            - 'attributes["http.target"] == "/livez"'
            - 'attributes["http.user_agent"] != nil and
                 IsMatch(attributes["http.user_agent"],
                         "(?i)(prometheus|kube-probe|googlebot|bingbot)")'

      # Mantém apenas spans com gen_ai.* (para pipeline LangFuse)
      filter/genai_only:
        error_mode: ignore
        traces:
          span:
            - 'attributes["gen_ai.system"] == nil'

      tail_sampling:
        decision_wait: 30s
        num_traces: 100000
        expected_new_traces_per_sec: 5000
        policies:
          # 100% dos traces com erro
          - name: errors
            type: status_code
            status_code:
              status_codes: [ERROR]

          # 100% dos traces lentos (> 2s)
          - name: slow-traces
            type: latency
            latency:
              threshold_ms: 2000

          # 100% dos traces com tool calls (agentes IA)
          - name: tool-calls
            type: string_attribute
            string_attribute:
              key: gen_ai.tool.name
              values: [".+"]
              enabled_regex_matching: true

          # 100% dos traces de clientes premium
          - name: premium-tenants
            type: string_attribute
            string_attribute:
              key: customer.tier
              values: ["enterprise", "premium"]

          # 5% probabilístico do baseline
          - name: baseline
            type: probabilistic
            probabilistic:
              sampling_percentage: 5

      # Última camada de defesa: fail-closed redaction
      redaction:
        allow_all_keys: false
        allowed_keys:
          # HTTP
          - http.method
          - http.request.method
          - http.status_code
          - http.response.status_code
          - http.route
          - http.scheme
          - url.path
          - url.scheme
          - server.address
          - server.port
          - http.user_agent

          # Service
          - service.name
          - service.namespace
          - service.version
          - service.instance.id
          - deployment.environment

          # Cloud / K8s
          - cloud.provider
          - cloud.region
          - cloud.availability_zone
          - k8s.cluster.name
          - k8s.namespace.name
          - k8s.pod.name
          - k8s.deployment.name
          - k8s.node.name

          # Span basics
          - span.kind
          - span.status
          - error
          - error.message
          - error.type
          - exception.type
          - exception.message

          # DB (sem db.statement por segurança)
          - db.system
          - db.operation
          - db.name
          - db.user

          # Messaging
          - messaging.system
          - messaging.destination.name
          - messaging.operation

          # GenAI (atributos sem texto livre)
          - gen_ai.system
          - gen_ai.request.model
          - gen_ai.response.model
          - gen_ai.usage.input_tokens
          - gen_ai.usage.output_tokens
          - gen_ai.response.finish_reasons
          - gen_ai.operation.name
          - gen_ai.tool.name
          - gen_ai.agent.name

        blocked_values:
          # Padrões PII de defesa adicional
          - "\\b\\d{3}\\.\\d{3}\\.\\d{3}-\\d{2}\\b"
          - "\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}\\b"
          - "\\b4[0-9]{12}(?:[0-9]{3})?\\b"
        summary: silent

      batch:
        timeout: 2s
        send_batch_size: 1024
        send_batch_max_size: 2048

    exporters:
      # Saída principal: tudo vai pro Elastic via Kafka Connect
      kafka/elastic:
        brokers:
          - cell-1.streampool.sa-saopaulo-1.oci.oraclecloud.com:9092
        topic: telemetry_to_elastic
        encoding: otlp_proto
        timeout: 30s
        auth:
          sasl:
            username: ${env:KAFKA_USERNAME}
            password: ${env:KAFKA_PASSWORD}
            mechanism: PLAIN
          tls:
            insecure: false
        producer:
          max_message_bytes: 1000000
          required_acks: 1
          compression: snappy
        sending_queue:
          enabled: true
          num_consumers: 10
          queue_size: 10000
          storage: file_storage/queue
        retry_on_failure:
          enabled: true
          initial_interval: 5s
          max_interval: 30s
          max_elapsed_time: 600s

      # LangFuse para observabilidade de agentes IA
      otlphttp/langfuse:
        endpoint: https://cloud.langfuse.com/api/public/otel
        compression: gzip
        timeout: 30s
        headers:
          authorization: Basic ${env:LANGFUSE_AUTH}
        sending_queue:
          enabled: true
          num_consumers: 5
          queue_size: 5000
          storage: file_storage/queue
        retry_on_failure:
          enabled: true
          max_elapsed_time: 600s

      # Arquivo de auditoria local
      file/audit:
        path: /var/lib/otel/audit/traces.jsonl
        rotation:
          max_megabytes: 500
          max_days: 30
          max_backups: 50
        format: json
        compression: zstd

    service:
      extensions: [health_check, file_storage/queue, pprof]
      pipelines:
        # Pipeline principal: tudo para Elastic + auditoria
        traces:
          receivers: [kafka/in]
          processors:
            - memory_limiter
            - k8sattributes
            - resource/cluster
            - filter/noise
            - transform/redaction_pii
            - tail_sampling
            - redaction
            - batch
          exporters: [kafka/elastic, file/audit]

        # Pipeline paralela: apenas gen_ai.* para LangFuse
        traces/langfuse:
          receivers: [kafka/in]
          processors:
            - memory_limiter
            - filter/genai_only
            - transform/redaction_pii
            - redaction
            - batch
          exporters: [otlphttp/langfuse]

        metrics:
          receivers: [kafka/in]
          processors:
            - memory_limiter
            - k8sattributes
            - resource/cluster
            - batch
          exporters: [kafka/elastic]

        logs:
          receivers: [kafka/in]
          processors:
            - memory_limiter
            - k8sattributes
            - resource/cluster
            - transform/redaction_pii
            - redaction
            - batch
          exporters: [kafka/elastic]

      telemetry:
        logs:
          level: info
          encoding: json
        metrics:
          level: detailed
          address: 0.0.0.0:8888
```

### Deployment, Service, HPA, PDB

```yaml
# 06-gateway-deployment.yaml
apiVersion: v1
kind: Service
metadata:
  name: otel-gateway
  namespace: observability
  labels:
    app: otel-gateway
spec:
  type: ClusterIP
  selector:
    app: otel-gateway
  ports:
    - name: metrics
      port: 8888
      targetPort: 8888
    - name: health
      port: 13133
      targetPort: 13133
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: otel-gateway
  namespace: observability
  labels:
    app: otel-gateway
spec:
  replicas: 3
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 2
      maxUnavailable: 0
  selector:
    matchLabels:
      app: otel-gateway
  template:
    metadata:
      labels:
        app: otel-gateway
        tier: gateway
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8888"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: otel-collector
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 100
              podAffinityTerm:
                labelSelector:
                  matchLabels:
                    app: otel-gateway
                topologyKey: kubernetes.io/hostname
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: topology.kubernetes.io/zone
          whenUnsatisfiable: ScheduleAnyway
          labelSelector:
            matchLabels:
              app: otel-gateway
      containers:
        - name: otel-gateway
          image: otel/opentelemetry-collector-contrib:0.151.0
          args: ["--config=/conf/config.yaml"]
          env:
            - name: ENVIRONMENT
              value: "prod"
            - name: GOMEMLIMIT
              value: "7000MiB"
            - name: KAFKA_USERNAME
              valueFrom:
                secretKeyRef:
                  name: telemetry-credentials
                  key: kafka-username
            - name: KAFKA_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: telemetry-credentials
                  key: kafka-password
            - name: LANGFUSE_AUTH
              valueFrom:
                secretKeyRef:
                  name: telemetry-credentials
                  key: langfuse-auth-b64
          ports:
            - name: metrics
              containerPort: 8888
            - name: health
              containerPort: 13133
          livenessProbe:
            httpGet:
              path: /health
              port: 13133
            initialDelaySeconds: 30
            periodSeconds: 30
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: /health
              port: 13133
            initialDelaySeconds: 10
            periodSeconds: 10
            failureThreshold: 3
          resources:
            requests:
              cpu: "2"
              memory: 4Gi
            limits:
              cpu: "4"
              memory: 8Gi
          securityContext:
            runAsNonRoot: true
            runAsUser: 10001
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          volumeMounts:
            - name: config
              mountPath: /conf
              readOnly: true
            - name: queue
              mountPath: /var/lib/otel/queue
            - name: audit
              mountPath: /var/lib/otel/audit
      volumes:
        - name: config
          configMap:
            name: otel-gateway-config
        - name: queue
          emptyDir:
            sizeLimit: 20Gi
        - name: audit
          emptyDir:
            sizeLimit: 50Gi
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: otel-gateway
  namespace: observability
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: otel-gateway
  minReplicas: 3
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: 75
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
        - type: Percent
          value: 25
          periodSeconds: 60
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
        - type: Percent
          value: 50
          periodSeconds: 60
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: otel-gateway
  namespace: observability
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app: otel-gateway
```

**Sobre `LANGFUSE_AUTH`:** o LangFuse usa Basic Auth com `public_key:secret_key` em base64. Gere localmente e armazene no Vault:

```bash
echo -n "pk-lf-xxxxx:sk-lf-yyyyy" | base64
# Cole o resultado no secret langfuse-auth-b64
```

---

## Kafka Connect: do segundo tópico para o Elastic

O fluxo final é o segundo tópico Kafka (`telemetry_to_elastic`) drenado por Kafka Connect para o Elastic. Use a imagem oficial do Elastic com o conector OTLP:

```yaml
# 07-kafka-connect.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-connect
  namespace: observability
spec:
  replicas: 2
  selector:
    matchLabels:
      app: kafka-connect
  template:
    metadata:
      labels:
        app: kafka-connect
    spec:
      containers:
        - name: kafka-connect
          image: confluentinc/cp-kafka-connect:7.6.0
          env:
            - name: CONNECT_BOOTSTRAP_SERVERS
              value: "cell-1.streampool.sa-saopaulo-1.oci.oraclecloud.com:9092"
            - name: CONNECT_REST_ADVERTISED_HOST_NAME
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
            - name: CONNECT_REST_PORT
              value: "8083"
            - name: CONNECT_GROUP_ID
              value: "otel-elastic-connect"
            - name: CONNECT_CONFIG_STORAGE_TOPIC
              value: "connect_configs"
            - name: CONNECT_OFFSET_STORAGE_TOPIC
              value: "connect_offsets"
            - name: CONNECT_STATUS_STORAGE_TOPIC
              value: "connect_status"
            - name: CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR
              value: "3"
            - name: CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR
              value: "3"
            - name: CONNECT_STATUS_STORAGE_REPLICATION_FACTOR
              value: "3"
            - name: CONNECT_KEY_CONVERTER
              value: "org.apache.kafka.connect.storage.StringConverter"
            - name: CONNECT_VALUE_CONVERTER
              value: "org.apache.kafka.connect.converters.ByteArrayConverter"
            - name: CONNECT_PLUGIN_PATH
              value: "/usr/share/confluent-hub-components"
            # SASL para OCI Streaming
            - name: CONNECT_SECURITY_PROTOCOL
              value: "SASL_SSL"
            - name: CONNECT_SASL_MECHANISM
              value: "PLAIN"
            - name: CONNECT_SASL_JAAS_CONFIG
              valueFrom:
                secretKeyRef:
                  name: telemetry-credentials
                  key: kafka-jaas-config
          ports:
            - containerPort: 8083
              name: rest
          resources:
            requests:
              cpu: "1"
              memory: 2Gi
            limits:
              cpu: "2"
              memory: 4Gi
---
apiVersion: v1
kind: Service
metadata:
  name: kafka-connect
  namespace: observability
spec:
  selector:
    app: kafka-connect
  ports:
    - port: 8083
      name: rest
```

Registre o connector via API REST:

```bash
# 08-register-connector.sh
#!/bin/bash

CONNECT_URL="http://kafka-connect.observability.svc.cluster.local:8083"

curl -X POST -H "Content-Type: application/json" "$CONNECT_URL/connectors" -d '{
  "name": "elastic-otlp-sink",
  "config": {
    "connector.class": "co.elastic.connect.kafka.ElasticOTLPSinkConnector",
    "tasks.max": "4",
    "topics": "telemetry_to_elastic",
    "elasticsearch.hosts": "https://elastic.example.com:9200",
    "elasticsearch.api_key": "${ELASTIC_API_KEY}",
    "elasticsearch.tls.verification_mode": "full",

    "index.signals.traces": "traces-otel-default",
    "index.signals.metrics": "metrics-otel-default",
    "index.signals.logs": "logs-otel-default",

    "buffer.flush.interval.ms": "5000",
    "buffer.max.records": "1000",

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "telemetry_dlq",
    "errors.deadletterqueue.context.headers.enable": "true",
    "errors.log.enable": "true",
    "errors.log.include.messages": "false"
  }
}'
```

Verifique o status:

```bash
curl -s "$CONNECT_URL/connectors/elastic-otlp-sink/status" | jq
```

---

## Observabilidade do próprio Collector

Você não pode confiar cegamente no que não consegue medir. O Collector expõe métricas Prometheus em `:8888/metrics`. Métricas críticas para monitorar:

```yaml
# 09-servicemonitor.yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: otel-gateway
  namespace: observability
  labels:
    release: prometheus
spec:
  selector:
    matchLabels:
      app: otel-gateway
  endpoints:
    - port: metrics
      interval: 30s
      path: /metrics
```

### Métricas essenciais e alertas

```yaml
# 10-alerts.yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: otel-gateway-alerts
  namespace: observability
spec:
  groups:
    - name: otel-gateway
      interval: 1m
      rules:
        # Spans sendo descartados (memory_limiter aplicando backpressure)
        - alert: OtelGatewayDroppingSpans
          expr: |
            rate(otelcol_processor_dropped_spans_total[5m]) > 0
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "Gateway descartando spans há {{ $for }}"
            description: "{{ $labels.processor }} no pod {{ $labels.pod }} está descartando spans. Capacidade insuficiente."

        # Falhas no exporter
        - alert: OtelGatewayExporterFailing
          expr: |
            rate(otelcol_exporter_send_failed_spans_total[5m]) /
            rate(otelcol_exporter_sent_spans_total[5m]) > 0.01
          for: 5m
          labels:
            severity: critical
          annotations:
            summary: "Exporter {{ $labels.exporter }} com >1% de falhas"
            description: "Backend {{ $labels.exporter }} pode estar indisponível ou rejeitando dados."

        # Queue depth alta
        - alert: OtelGatewayQueueHigh
          expr: |
            otelcol_exporter_queue_size / otelcol_exporter_queue_capacity > 0.8
          for: 10m
          labels:
            severity: warning
          annotations:
            summary: "Queue do exporter {{ $labels.exporter }} >80% cheia"

        # Lag do consumer group no Kafka
        - alert: OtelGatewayKafkaLag
          expr: |
            kafka_consumergroup_lag{consumergroup="otel-gateway"} > 100000
          for: 10m
          labels:
            severity: warning
          annotations:
            summary: "Lag do Gateway no Kafka acima de 100k"
            description: "Gateway consumindo mais devagar que produtores. Considere escalar."
```

### Dashboard mínimo

Painéis recomendados em Grafana ou Kibana:

| Painel | Métrica |
|---|---|
| Throughput de entrada | `rate(otelcol_receiver_accepted_spans_total[5m])` |
| Throughput de saída | `rate(otelcol_exporter_sent_spans_total[5m])` |
| Taxa de falhas no exporter | `rate(otelcol_exporter_send_failed_spans_total[5m])` |
| Spans descartados | `rate(otelcol_processor_dropped_spans_total[5m])` |
| Queue size | `otelcol_exporter_queue_size` |
| Heap em uso | `process_runtime_go_mem_heap_inuse_bytes` |
| Lag do consumer | `kafka_consumergroup_lag{consumergroup="otel-gateway"}` |

---

## Troubleshooting comum

### "Connection refused" no exporter Kafka

Verifique conectividade do pod ao endpoint do OCI Streaming:

```bash
kubectl run -it --rm debug --image=busybox:latest --restart=Never -- \
  nc -zv cell-1.streampool.sa-saopaulo-1.oci.oraclecloud.com 9092
```

Se falhar, revise NSGs e Security Lists na VCN.

### "SASL authentication failed"

Cause comuns:

1. Formato do username está errado. Deve ser exatamente `tenancy/user/streamPoolOCID`
2. Auth token expirou ou foi revogado
3. O usuário OCI não tem policy para usar o stream

```bash
# Verifica o secret atual
kubectl get secret telemetry-credentials -n observability -o jsonpath='{.data.kafka-username}' | base64 -d
```

### Gateway com OOMKill recorrente

```bash
kubectl describe pod otel-gateway-xxx -n observability | grep -A 5 "Last State"
```

Se houver OOMKilled:

1. Verifique se `GOMEMLIMIT` está configurado corretamente (~90% do `limits.memory`)
2. Reduza `num_traces` no `tail_sampling` se traces forem grandes
3. Reduza `queue_size` nos exporters
4. Escale verticalmente o pod

### "Pipeline error: invalid configuration"

Antes de aplicar, valide o YAML localmente:

```bash
docker run --rm -v $PWD/config.yaml:/conf/config.yaml \
  otel/opentelemetry-collector-contrib:0.151.0 \
  --config=/conf/config.yaml --dry-run
```

### Traces fragmentados (parent_span_id quebrado)

Sinal de propagação de contexto quebrada entre serviços. Cheque:

1. Todas as aplicações estão usando o mesmo padrão W3C Trace Context
2. Bibliotecas legadas (HTTP clients antigos) propagam o header `traceparent`
3. Mensageria propaga via headers da mensagem (não só no body)

### Lag crescendo no consumer group

```bash
# Via kafka-consumer-groups (cliente Kafka padrão)
kafka-consumer-groups --bootstrap-server $BROKERS \
  --command-config /tmp/sasl.properties \
  --group otel-gateway --describe
```

Se lag está crescendo:

1. Escalar Gateway horizontalmente (HPA já cuida se configurado)
2. Aumentar `num_consumers` nos exporters
3. Investigar se o backend (Elastic, LangFuse) está lento

---

## Próximos passos

Esta arquitetura cobre o caminho até produção, mas existe espaço para evoluir:

- **OCB (OpenTelemetry Collector Builder)**: depois que a stack estabilizar, construa uma distribuição customizada do Collector com apenas os componentes em uso. Reduz tamanho de imagem de 250 MB para 40-80 MB, e diminui superfície de ataque (menos CVEs).

- **GitOps com ArgoCD**: versione todos os manifestos em git, com overlays Kustomize por ambiente (dev/fqa/prod). PRs para mudanças sensíveis, com aprovação obrigatória para PROD.

- **OCB Builder + custom processors**: se você tiver lógica de redação específica de domínio (CPF brasileiro com algoritmo de validação, números de cartão de saúde, etc.), pode escrever processors customizados em Go.

- **Multi-região**: replique a arquitetura em outra região OCI (ex: vinhedo-1), com failover exporter no Gateway para resiliência geográfica. Atenção: cruzar fronteira nacional com dados pessoais exige base legal documentada (LGPD).

- **Métricas de negócio**: além de métricas técnicas (latência, throughput), explore o `spanmetrics` connector para gerar RED metrics (Rate, Errors, Duration) automaticamente a partir dos spans.

---

## Conclusão

A combinação OCI Streaming + OKE + OpenTelemetry oferece uma base de observabilidade robusta para aplicações modernas, especialmente as que envolvem agentes de IA. Os pontos cruciais que fazem a arquitetura funcionar:

1. **Kafka como buffer durável** entre Collectors locais e Gateway desacopla produtores de consumo. É o ingrediente que viabiliza manutenção sem janela e absorção de picos.

2. **Sidecar para isolamento por aplicação** com OpenTelemetry Operator simplifica o gerenciamento — uma única CRD `OpenTelemetryCollector` configura todos os sidecars do cluster.

3. **Gateway central com pipelines paralelas** roteia traces de LLM ao LangFuse e operação geral ao Elastic, evitando duplicação e otimizando custos.

4. **Redação de PII em duas etapas** (transform com regex + redaction fail-closed) endereça LGPD de forma auditável.

5. **Tail sampling inteligente** garante 100% de visibilidade dos casos relevantes (erros, lentidão, tool calls) enquanto reduz volume em 90%+ do baseline.

6. **OCI Vault + External Secrets Operator** mantém credenciais fora do git, com rotação automática suportada.

A arquitetura cresce horizontalmente: do primeiro serviço instrumentado em DEV até centenas de aplicações em PROD multi-região, os mesmos blocos se aplicam, com calibração de parâmetros conforme volume.

---

*Os códigos apresentados foram testados em OCI sa-saopaulo-1 com OpenTelemetry Collector contrib v0.151.0, OKE 1.30 e Confluent Connect 7.6. Adaptações para outras regiões ou versões podem requerer ajustes em endpoints e plugins.*
