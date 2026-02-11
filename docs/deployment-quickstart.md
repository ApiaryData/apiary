# Quick Deployment Guide

This is a quick reference for deploying Apiary on new compute nodes. For detailed instructions, see the full deployment guides:
- [Raspberry Pi Deployment](deployment-raspberry-pi.md)
- [Cloud Container Deployment](deployment-containers.md)

## Scenario 1: Initialize a New Apiary Instance

### On Raspberry Pi

```bash
# 1. Install dependencies
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
sudo apt install -y python3 python3-pip git build-essential libssl-dev pkg-config
pip3 install maturin

# 2. Build Apiary
git clone https://github.com/ApiaryData/apiary.git
cd apiary
cargo build --release --workspace
maturin develop --release

# 3. Start Apiary (solo mode with local storage)
python3 << 'EOF'
from apiary import Apiary

ap = Apiary("my_apiary")
ap.start()

status = ap.status()
print(f"✓ Apiary initialized: {status['cores']} cores, {status['memory_gb']:.2f}GB")

# Create test data
ap.create_hive("test")
ap.create_box("test", "data")
print("✓ Ready to use!")

ap.shutdown()
EOF
```

### With Docker

```bash
# 1. Build image
git clone https://github.com/ApiaryData/apiary.git
cd apiary
docker build -t apiary:latest .

# 2. Start Apiary
docker run -it --rm \
  -v apiary-data:/home/apiary/data \
  apiary:latest \
  python3 -c "
from apiary import Apiary
ap = Apiary('my_apiary')
ap.start()
print('✓ Apiary initialized')
print(ap.status())
"
```

### With Docker Compose + MinIO

```bash
# 1. Create docker-compose.yml (includes MinIO for S3-compatible storage)
cat > docker-compose.yml << 'YAML'
version: '3.8'
services:
  minio:
    image: minio/minio:latest
    ports: ["9000:9000", "9001:9001"]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    volumes:
      - minio-data:/data

  minio-setup:
    image: minio/mc:latest
    depends_on: [minio]
    entrypoint: >
      /bin/sh -c "
      sleep 5;
      /usr/bin/mc config host add myminio http://minio:9000 minioadmin minioadmin;
      /usr/bin/mc mb myminio/apiary --ignore-existing;
      exit 0;
      "

  apiary-node:
    image: apiary:latest
    depends_on: [minio, minio-setup]
    environment:
      AWS_ACCESS_KEY_ID: minioadmin
      AWS_SECRET_ACCESS_KEY: minioadmin
      AWS_ENDPOINT_URL: http://minio:9000
    command: python3 -c "from apiary import Apiary; import time; ap = Apiary('production', storage='s3://apiary/data'); ap.start(); print('Node started:', ap.status()); time.sleep(3600)"

volumes:
  minio-data:
YAML

# 2. Start services
docker-compose up -d

# 3. Verify
docker-compose logs apiary-node
```

## Scenario 2: Add a New Compute Node to Existing Apiary

The key principle: **All nodes connect to the same storage bucket. That's it.**

### Add Raspberry Pi to Existing Swarm

```bash
# 1. On the new Pi, install and build Apiary (same as above)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
sudo apt install -y python3 python3-pip git build-essential libssl-dev pkg-config
pip3 install maturin

git clone https://github.com/ApiaryData/apiary.git
cd apiary
cargo build --release --workspace
maturin develop --release

# 2. Create node script pointing to SAME storage as other nodes
cat > ~/apiary-node.py << 'EOF'
import os
from apiary import Apiary
import time

# ⚠️ IMPORTANT: Use the SAME storage URL as your existing nodes
os.environ['AWS_ACCESS_KEY_ID'] = 'your_access_key'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'your_secret_key'
os.environ['AWS_ENDPOINT_URL'] = 'http://192.168.1.100:9000'  # Your MinIO/S3 endpoint

ap = Apiary("production", storage="s3://apiary/data")  # SAME bucket as other nodes
ap.start()

print(f"✓ Node joined swarm: {ap.status()['node_id']}")

# Verify swarm membership
swarm = ap.swarm_status()
print(f"✓ Swarm has {swarm['alive']} alive nodes")
for node in swarm['nodes']:
    print(f"  - {node['node_id']}: {node['state']}")

# Keep running
try:
    while True:
        time.sleep(60)
except KeyboardInterrupt:
    ap.shutdown()
EOF

# 3. Start the node
python3 ~/apiary-node.py

# 4. (Optional) Run as systemd service
sudo tee /etc/systemd/system/apiary.service > /dev/null << 'SERVICE'
[Unit]
Description=Apiary Node
After=network.target

[Service]
Type=simple
User=pi
WorkingDirectory=/home/pi
Environment="PATH=/home/pi/.cargo/bin:/usr/local/bin:/usr/bin:/bin"
ExecStart=/usr/bin/python3 /home/pi/apiary-node.py
Restart=always

[Install]
WantedBy=multi-user.target
SERVICE

sudo systemctl daemon-reload
sudo systemctl enable apiary.service
sudo systemctl start apiary.service
```

### Add Docker Container to Existing Swarm

```bash
# Just run a new container with the SAME storage URL
docker run -d \
  --name apiary-node-new \
  -e AWS_ACCESS_KEY_ID=your_access_key \
  -e AWS_SECRET_ACCESS_KEY=your_secret_key \
  -e AWS_REGION=us-east-1 \
  apiary:latest \
  python3 -c "
from apiary import Apiary
import time
ap = Apiary('production', storage='s3://my-bucket/apiary')  # SAME bucket
ap.start()
print('Node started:', ap.status())
swarm = ap.swarm_status()
print(f'Joined swarm with {swarm[\"alive\"]} nodes')
while True: time.sleep(60)
"
```

### Add Kubernetes Pod to Existing Swarm

```bash
# Just scale the deployment
kubectl scale deployment apiary-nodes -n apiary --replicas=5

# New pods automatically join the swarm (they all use the same storage URL)
```

## How Node Discovery Works

When you start a new node with the same storage URL:

1. **Node writes heartbeat**: New node writes `/storage/_heartbeats/node_{id}.json`
2. **Other nodes discover it**: Within 5-10 seconds, other nodes read heartbeat table
3. **Node becomes active**: Coordinator starts assigning query tasks to it
4. **Zero configuration**: No restarts, no config changes on existing nodes needed

## Verification Commands

### Check if Node Joined Swarm

From **any** node (existing or new):

```python
from apiary import Apiary
import os

# Configure storage credentials
os.environ['AWS_ACCESS_KEY_ID'] = 'your_key'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'your_secret'

ap = Apiary("production", storage="s3://apiary/data")
ap.start()

swarm = ap.swarm_status()
print(f"Total nodes: {swarm['alive']} alive, {swarm['suspect']} suspect, {swarm['dead']} dead")
print(f"Total compute: {swarm['total_bees']} bees, {swarm['total_memory_gb']:.2f}GB")

for node in swarm['nodes']:
    print(f"{node['node_id']}: {node['state']} - {node['cores']} cores, {node['memory_gb']:.2f}GB")

ap.shutdown()
```

### Test Distributed Query

```python
from apiary import Apiary
import pyarrow as pa
import os

os.environ['AWS_ACCESS_KEY_ID'] = 'your_key'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'your_secret'

ap = Apiary("production", storage="s3://apiary/data")
ap.start()

# Write test data
if "test" not in ap.list_hives():
    ap.create_hive("test")
    ap.create_box("test", "data")
    ap.create_frame("test", "data", "numbers", {"id": "int64", "value": "float64"})

# Write data that will be distributed across nodes
table = pa.table({
    "id": list(range(10000)),
    "value": [float(i) * 1.5 for i in range(10000)],
})

sink = pa.BufferOutputStream()
writer = pa.ipc.new_stream_writer(sink, table.schema)
writer.write_table(table)
writer.close()

result = ap.write_to_frame("test", "data", "numbers", sink.getvalue().to_pybytes())
print(f"✓ Wrote {result['rows_written']} rows")

# Run query (will be distributed if multiple nodes)
result_bytes = ap.sql("SELECT COUNT(*), AVG(value) FROM test.data.numbers")
reader = pa.ipc.open_stream(result_bytes)
result = reader.read_all()
print(f"✓ Query result: {result.to_pandas()}")

ap.shutdown()
```

## Common Storage Configurations

### MinIO (Self-Hosted)

```python
import os
os.environ['AWS_ACCESS_KEY_ID'] = 'minioadmin'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin'
os.environ['AWS_ENDPOINT_URL'] = 'http://192.168.1.100:9000'

ap = Apiary("production", storage="s3://apiary/data")
```

### AWS S3

```python
# Option 1: Environment variables
import os
# ⚠️ Replace with your actual AWS credentials (these are example values from AWS documentation)
os.environ['AWS_ACCESS_KEY_ID'] = 'AKIAIOSFODNN7EXAMPLE'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
os.environ['AWS_REGION'] = 'us-east-1'

ap = Apiary("production", storage="s3://my-bucket/apiary")

# Option 2: AWS credentials file (~/.aws/credentials)
# Just configure with: aws configure
ap = Apiary("production", storage="s3://my-bucket/apiary")
```

### Google Cloud Storage

```python
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/path/to/credentials.json'

ap = Apiary("production", storage="gs://my-bucket/apiary")
```

### Local Filesystem (Solo Mode)

```python
# No credentials needed
ap = Apiary("my_apiary")  # Uses local storage in ~/.apiary/my_apiary
```

## Troubleshooting

### Node not appearing in swarm

1. **Check storage connectivity**:
   ```bash
   # For MinIO
   curl http://192.168.1.100:9000
   
   # For S3
   aws s3 ls s3://my-bucket/apiary/
   ```

2. **Verify credentials**:
   ```python
   import boto3
   s3 = boto3.client('s3')
   print(s3.list_buckets())  # Should not error
   ```

3. **Check storage URL is identical** on all nodes (including trailing path)

4. **Wait 5-10 seconds** for heartbeat discovery

### "Access Denied" errors

- Verify credentials match across all nodes
- Check IAM permissions for S3 bucket
- For MinIO, verify user has read/write access to bucket

### Node shows as "dead"

- Check system time is synchronized (NTP)
- Verify node process is actually running
- Check network connectivity to storage

## Next Steps

- **Full Pi Guide**: [Raspberry Pi Deployment](deployment-raspberry-pi.md)
- **Full Container Guide**: [Cloud Container Deployment](deployment-containers.md)
- **Python API**: [Python SDK Reference](python-sdk.md)
- **Concepts**: [Apiary Concepts](concepts.md)
