# Cloud Container Deployment Guide

This guide covers deploying Apiary in containerized environments using Docker and Kubernetes. Containers provide a consistent runtime environment and make it easy to deploy Apiary on any cloud platform.

> **Quick Start**: For rapid deployment instructions, see the [Quick Deployment Guide](deployment-quickstart.md).

## Prerequisites

- **Docker** 20.10+ ([Install Docker](https://docs.docker.com/get-docker/))
- **Docker Compose** 2.0+ (included with Docker Desktop)
- **Kubernetes** 1.20+ (for k8s deployments)
- **kubectl** configured for your cluster (for k8s deployments)
- Cloud storage access (S3, GCS, or MinIO)

## Quick Start with Docker

### 1. Build or Pull the Image

The repository includes a production-ready `Dockerfile` in the project root.

```bash
# Build the image
docker build -t apiary:latest .

# Or build for specific platform
docker build --platform linux/amd64 -t apiary:latest .
docker build --platform linux/arm64 -t apiary:latest .

# Multi-arch build (requires buildx)
docker buildx build --platform linux/amd64,linux/arm64 -t apiary:latest .
```

### 3. Run a Single Node

```bash
# Solo mode (local storage)
docker run -it --rm \
  -v apiary-data:/home/apiary/data \
  apiary:latest \
  python3 -c "
from apiary import Apiary
ap = Apiary('my_apiary')
ap.start()
print(ap.status())
ap.shutdown()
"

# Multi-node mode (S3 storage)
docker run -it --rm \
  -e AWS_ACCESS_KEY_ID=your_access_key \
  -e AWS_SECRET_ACCESS_KEY=your_secret_key \
  -e AWS_REGION=us-east-1 \
  apiary:latest \
  python3 -c "
from apiary import Apiary
ap = Apiary('production', storage='s3://my-bucket/apiary')
ap.start()
print(ap.status())
import time
time.sleep(3600)  # Run for 1 hour
"
```

## Docker Compose Deployment

### Basic Multi-Node Setup

The repository includes a ready-to-use `docker-compose.yml` in the project root. You can use it directly or customise it for your environment.

> **⚠️ Security Warning**: The default MinIO credentials (`minioadmin`/`minioadmin`) are for development only. **Always use strong credentials in production** by setting `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` environment variables.

```bash
# Use the provided docker-compose.yml
docker compose up -d

# Scale to multiple workers
docker compose up -d --scale apiary-node=3

# Override default credentials
MINIO_ROOT_USER=myuser MINIO_ROOT_PASSWORD=mypassword docker compose up -d
```

You can also download the compose file independently:

```bash
curl -fsSL https://raw.githubusercontent.com/ApiaryData/apiary/main/docker-compose.yml -o docker-compose.yml
docker compose up -d
```

### Start the Swarm

```bash
# Build the image first (if not using a pre-built image)
docker build -t apiary:latest .

# Start all services
docker compose up -d

# Scale to multiple nodes
docker compose up -d --scale apiary-node=3

# Check status
docker compose ps

# View logs
docker compose logs -f apiary-node

# Access MinIO console
# Open http://localhost:9001 in browser
```

### Add More Nodes

```bash
# Scale up the apiary nodes
docker compose up -d --scale apiary-node=5
```

The new nodes will automatically discover existing nodes through the shared storage and join the swarm.

## Kubernetes Deployment

### Create Kubernetes Resources

#### 1. Namespace and ConfigMap

`apiary-namespace.yaml`:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: apiary
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: apiary-config
  namespace: apiary
data:
  apiary-node.py: |
    #!/usr/bin/env python3
    import os
    import time
    import signal
    import sys
    from apiary import Apiary

    # Get storage URL from environment
    storage_url = os.environ.get('APIARY_STORAGE_URL', 's3://apiary/data')
    
    ap = Apiary("production", storage=storage_url)
    ap.start()

    status = ap.status()
    print(f"Node {status['node_id']} started: {status['cores']} cores, {status['memory_gb']:.2f}GB")

    def signal_handler(sig, frame):
        print("Shutting down gracefully...")
        ap.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        while True:
            time.sleep(60)
            swarm = ap.swarm_status()
            print(f"Swarm: {swarm['alive']} alive, {swarm['total_bees']} bees")
    except Exception as e:
        print(f"Error: {e}")
        ap.shutdown()
        sys.exit(1)
```

#### 2. Secret for Storage Credentials

`apiary-secret.yaml`:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: apiary-storage-secret
  namespace: apiary
type: Opaque
stringData:
  AWS_ACCESS_KEY_ID: "your_access_key_here"
  AWS_SECRET_ACCESS_KEY: "your_secret_key_here"
  AWS_REGION: "us-east-1"
  # For MinIO
  # AWS_ENDPOINT_URL: "http://minio.apiary.svc.cluster.local:9000"
```

#### 3. Deployment

`apiary-deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: apiary-nodes
  namespace: apiary
spec:
  replicas: 3
  selector:
    matchLabels:
      app: apiary
  template:
    metadata:
      labels:
        app: apiary
    spec:
      containers:
      - name: apiary
        image: apiary:latest
        imagePullPolicy: IfNotPresent
        command: ["python3", "/config/apiary-node.py"]
        env:
        - name: APIARY_STORAGE_URL
          value: "s3://my-bucket/apiary"
        - name: RUST_LOG
          value: "info"
        envFrom:
        - secretRef:
            name: apiary-storage-secret
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
        volumeMounts:
        - name: config
          mountPath: /config
        - name: cache
          mountPath: /home/apiary/cache
      volumes:
      - name: config
        configMap:
          name: apiary-config
          defaultMode: 0755
      - name: cache
        emptyDir:
          sizeLimit: 10Gi
```

#### 4. Optional: MinIO in Kubernetes

`minio-deployment.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: minio
  namespace: apiary
spec:
  ports:
  - port: 9000
    name: api
  - port: 9001
    name: console
  selector:
    app: minio
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  namespace: apiary
spec:
  replicas: 1
  selector:
    matchLabels:
      app: minio
  template:
    metadata:
      labels:
        app: minio
    spec:
      containers:
      - name: minio
        image: minio/minio:latest
        args:
        - server
        - /data
        - --console-address
        - ":9001"
        env:
        - name: MINIO_ROOT_USER
          value: "minioadmin"
        - name: MINIO_ROOT_PASSWORD
          value: "minioadmin"
        ports:
        - containerPort: 9000
        - containerPort: 9001
        volumeMounts:
        - name: data
          mountPath: /data
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: minio-pvc
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: minio-pvc
  namespace: apiary
spec:
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: 100Gi
```

### Deploy to Kubernetes

```bash
# Create namespace and resources
kubectl apply -f apiary-namespace.yaml
kubectl apply -f apiary-secret.yaml
kubectl apply -f apiary-deployment.yaml

# Optional: Deploy MinIO
kubectl apply -f minio-deployment.yaml

# Check deployment
kubectl get pods -n apiary
kubectl get deployments -n apiary

# View logs
kubectl logs -n apiary -l app=apiary -f

# Scale up/down
kubectl scale deployment apiary-nodes -n apiary --replicas=5
```

### Add New Nodes to Existing Swarm

Simply scale the deployment:

```bash
# Add 2 more nodes (from 3 to 5)
kubectl scale deployment apiary-nodes -n apiary --replicas=5

# Nodes will automatically join the swarm
# Verify by checking logs
kubectl logs -n apiary -l app=apiary --tail=20
```

## Cloud Platform Specific Guides

### AWS ECS (Elastic Container Service)

#### 1. Create Task Definition

```json
{
  "family": "apiary-node",
  "taskRoleArn": "arn:aws:iam::ACCOUNT_ID:role/apiary-task-role",
  "executionRoleArn": "arn:aws:iam::ACCOUNT_ID:role/ecsTaskExecutionRole",
  "networkMode": "awsvpc",
  "containerDefinitions": [
    {
      "name": "apiary",
      "image": "ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/apiary:latest",
      "memory": 4096,
      "cpu": 2048,
      "essential": true,
      "environment": [
        {
          "name": "APIARY_STORAGE_URL",
          "value": "s3://my-bucket/apiary"
        }
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/apiary",
          "awslogs-region": "us-east-1",
          "awslogs-stream-prefix": "apiary"
        }
      }
    }
  ],
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "2048",
  "memory": "4096"
}
```

#### 2. Create IAM Role

The task role needs S3 access:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-bucket/apiary/*",
        "arn:aws:s3:::my-bucket"
      ]
    }
  ]
}
```

#### 3. Create Service

```bash
# Create ECS cluster
aws ecs create-cluster --cluster-name apiary-cluster

# Register task definition
aws ecs register-task-definition --cli-input-json file://task-definition.json

# Create service with 3 nodes
aws ecs create-service \
  --cluster apiary-cluster \
  --service-name apiary-nodes \
  --task-definition apiary-node:1 \
  --desired-count 3 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-xxx],securityGroups=[sg-xxx],assignPublicIp=ENABLED}"
```

#### 4. Scale Service

```bash
# Scale to 5 nodes
aws ecs update-service \
  --cluster apiary-cluster \
  --service apiary-nodes \
  --desired-count 5
```

### Google Cloud Run

Cloud Run is good for serverless deployments, but Apiary needs long-running processes. Use GKE instead for production.

### Azure Container Instances

Similar to AWS ECS, create container groups:

```bash
# Create resource group
az group create --name apiary-rg --location eastus

# Create container group
az container create \
  --resource-group apiary-rg \
  --name apiary-node-1 \
  --image apiary:latest \
  --cpu 2 \
  --memory 4 \
  --environment-variables \
    APIARY_STORAGE_URL=s3://my-bucket/apiary \
    AWS_ACCESS_KEY_ID=xxx \
    AWS_SECRET_ACCESS_KEY=xxx
```

## Configuration Best Practices

### Resource Allocation

```yaml
# Minimum viable configuration
resources:
  requests:
    memory: "1Gi"
    cpu: "500m"
  limits:
    memory: "2Gi"
    cpu: "1000m"

# Recommended configuration
resources:
  requests:
    memory: "2Gi"
    cpu: "1000m"
  limits:
    memory: "4Gi"
    cpu: "2000m"

# High-performance configuration
resources:
  requests:
    memory: "4Gi"
    cpu: "2000m"
  limits:
    memory: "8Gi"
    cpu: "4000m"
```

### Storage Configuration

#### Local Cache Volume

```yaml
volumes:
- name: cache
  emptyDir:
    sizeLimit: 10Gi  # Adjust based on workload
```

#### Persistent Storage (for solo mode)

```yaml
volumes:
- name: data
  persistentVolumeClaim:
    claimName: apiary-data-pvc
```

### Environment Variables

```yaml
env:
- name: RUST_LOG
  value: "info"  # or "debug" for verbose logging
- name: APIARY_STORAGE_URL
  value: "s3://my-bucket/apiary"
- name: APIARY_HEARTBEAT_INTERVAL
  value: "5"  # seconds
- name: APIARY_CACHE_SIZE
  value: "2147483648"  # 2GB in bytes
```

## Monitoring and Observability

### Health Checks

Add liveness and readiness probes (when v2 HTTP API is available):

```yaml
livenessProbe:
  exec:
    command:
    - python3
    - -c
    - "from apiary import Apiary; ap = Apiary('production', storage='s3://bucket/apiary'); ap.start(); ap.shutdown()"
  initialDelaySeconds: 30
  periodSeconds: 60
  
readinessProbe:
  exec:
    command:
    - python3
    - -c
    - "from apiary import Apiary; print('ready')"
  initialDelaySeconds: 10
  periodSeconds: 30
```

### Logging

```yaml
# Use structured logging
env:
- name: RUST_LOG
  value: "apiary=info,apiary_storage=debug"
```

View logs:

```bash
# Docker Compose
docker-compose logs -f apiary-node-1

# Kubernetes
kubectl logs -n apiary -l app=apiary -f

# Docker
docker logs -f apiary-node-1
```

### Metrics

Create a sidecar container for metrics export (v2 feature):

```yaml
- name: metrics-exporter
  image: prom/statsd-exporter
  ports:
  - containerPort: 9102
```

## Troubleshooting

### Container Won't Start

```bash
# Check logs
docker logs apiary-node-1

# Check if image built correctly
docker run -it apiary:latest python3 -c "from apiary import Apiary; print('OK')"

# Check storage connectivity
docker run -it apiary:latest python3 -c "
import boto3
s3 = boto3.client('s3')
print(s3.list_buckets())
"
```

### Nodes Not Joining Swarm

```bash
# Verify storage configuration
kubectl exec -n apiary -it apiary-nodes-xxx -- env | grep AWS

# Check network connectivity
kubectl exec -n apiary -it apiary-nodes-xxx -- curl http://minio:9000

# Verify heartbeats are being written
# Check MinIO console or S3 bucket for _heartbeats/ directory
```

### High Memory Usage

```bash
# Check container memory
docker stats

# Kubernetes
kubectl top pods -n apiary

# Adjust memory limits in deployment
kubectl set resources deployment apiary-nodes -n apiary --limits=memory=8Gi
```

### Storage Errors

```bash
# Verify credentials
kubectl exec -n apiary -it apiary-nodes-xxx -- python3 -c "
import os
import boto3
print('Access Key:', os.environ.get('AWS_ACCESS_KEY_ID'))
s3 = boto3.client('s3')
try:
    print('Buckets:', s3.list_buckets())
except Exception as e:
    print('Error:', e)
"
```

## Performance Tuning

### CPU Allocation

- Allocate 1-2 CPU cores per node minimum
- More cores = more bees = higher parallelism
- Each bee needs adequate memory (~512MB-1GB)

### Memory Allocation

- Minimum: 1GB per core (bee)
- Recommended: 1.5-2GB per core
- Set appropriate limits to prevent OOM kills

### Network Optimization

- Use same region/zone as storage (S3, MinIO)
- Enable VPC endpoints for AWS S3
- Use private networking between nodes and storage

### Storage Optimization

- Use local ephemeral storage for cache
- Size cache volume appropriately (10-20GB per node)
- Enable compression in object storage

## Security

### Image Security

```bash
# Scan image for vulnerabilities
docker scan apiary:latest

# Use specific base image versions
FROM python:3.11.8-slim-bookworm
```

### Secrets Management

```bash
# Kubernetes secrets
kubectl create secret generic apiary-storage-secret \
  --from-literal=AWS_ACCESS_KEY_ID=xxx \
  --from-literal=AWS_SECRET_ACCESS_KEY=xxx \
  -n apiary

# Use external secrets operator
# Or cloud-native secrets (AWS Secrets Manager, GCP Secret Manager)
```

### Network Policies

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: apiary-network-policy
  namespace: apiary
spec:
  podSelector:
    matchLabels:
      app: apiary
  policyTypes:
  - Ingress
  - Egress
  egress:
  - to:
    - podSelector:
        matchLabels:
          app: minio
  - to:
    - namespaceSelector: {}
    ports:
    - protocol: TCP
      port: 443  # HTTPS for S3
```

### RBAC

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: apiary-sa
  namespace: apiary
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: apiary-role
  namespace: apiary
rules:
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: apiary-rolebinding
  namespace: apiary
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: apiary-role
subjects:
- kind: ServiceAccount
  name: apiary-sa
  namespace: apiary
```

## Next Steps

- See [Raspberry Pi Deployment](deployment-raspberry-pi.md) for edge deployments
- See [Python SDK Reference](python-sdk.md) for API details
- See [Architecture Summary](architecture-summary.md) for system design
- See [Swarm Coordination](architecture/03-swarm-coordination.md) for multi-node details

## Support

For issues or questions:
- GitHub Issues: https://github.com/ApiaryData/apiary/issues
- Documentation: https://github.com/ApiaryData/apiary/tree/main/docs
