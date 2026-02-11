# Raspberry Pi Deployment Guide

This guide covers deploying Apiary on Raspberry Pi devices, from a single node setup to adding multiple Pi devices to form a distributed swarm.

## Prerequisites

### Hardware Requirements

- **Raspberry Pi 4 or 5** (minimum 2GB RAM, 4GB+ recommended)
- **MicroSD card** (32GB+ recommended, Class 10 or better)
- **Power supply** (official Raspberry Pi power supply recommended)
- **Network connection** (Ethernet recommended for stability)
- **Optional**: External USB storage for better performance and capacity

### Recommended Models
- **Raspberry Pi 5** (8GB): Best performance
- **Raspberry Pi 4** (4GB or 8GB): Good balance of performance and cost
- **Raspberry Pi 4** (2GB): Minimum viable configuration

## Operating System Setup

### 1. Install Raspberry Pi OS

Use **Raspberry Pi OS Lite (64-bit)** for headless deployment:

```bash
# Download Raspberry Pi Imager from https://www.raspberrypi.com/software/
# Flash Raspberry Pi OS Lite (64-bit) to your SD card
# Enable SSH during imaging (via Raspberry Pi Imager settings)
```

### 2. Initial System Configuration

After booting your Pi for the first time:

```bash
# Connect via SSH
ssh pi@raspberrypi.local

# Update system packages
sudo apt update && sudo apt upgrade -y

# Set hostname (optional but recommended for multi-node)
sudo raspi-config
# Navigate to: System Options > Hostname
# Set a unique name like: apiary-pi-01

# Reboot to apply changes
sudo reboot
```

### 3. Install Dependencies

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# Install Python and build dependencies
sudo apt install -y python3 python3-pip python3-venv git \
    build-essential libssl-dev pkg-config

# Install maturin for Python/Rust bridge
pip3 install maturin
```

## Building Apiary

### Option 1: Build on Raspberry Pi (Recommended for single device)

```bash
# Clone the repository
git clone https://github.com/ApiaryData/apiary.git
cd apiary

# Build the Rust workspace (this will take 20-40 minutes on a Pi)
cargo build --release --workspace

# Build and install the Python package
maturin develop --release

# Verify installation
python3 -c "from apiary import Apiary; print('Apiary installed successfully')"
```

### Option 2: Cross-Compile from x86_64 Linux (Faster for multiple devices)

If you're deploying to multiple Pi devices, cross-compile on a faster machine:

```bash
# On your build machine (x86_64 Linux)
# Install cross-compilation tools
sudo apt install gcc-aarch64-linux-gnu

# Add ARM64 target to Rust
rustup target add aarch64-unknown-linux-gnu

# Clone and build
git clone https://github.com/ApiaryData/apiary.git
cd apiary

# Configure cargo for cross-compilation
mkdir -p .cargo
cat > .cargo/config.toml << 'EOF'
[target.aarch64-unknown-linux-gnu]
linker = "aarch64-linux-gnu-gcc"
EOF

# Build for ARM64
cargo build --release --workspace --target aarch64-unknown-linux-gnu

# Build Python wheel for ARM64
maturin build --release --target aarch64-unknown-linux-gnu

# Copy the wheel to your Pi
scp target/wheels/*.whl pi@apiary-pi-01:~/
```

Then on the Raspberry Pi:

```bash
# Install the wheel
pip3 install ~/apiary-*.whl
```

## Configuration

### Solo Mode (Single Node)

Create a configuration file for local development:

```bash
# Create data directory
mkdir -p ~/apiary-data

# Create a simple test script
cat > ~/apiary-test.py << 'EOF'
from apiary import Apiary
import pyarrow as pa

# Initialize Apiary in solo mode (uses local filesystem)
ap = Apiary("my_apiary")
ap.start()

# Check status
status = ap.status()
print(f"Apiary running on {status['cores']} cores with {status['memory_gb']:.2f}GB memory")

# Create a simple table
ap.create_hive("test")
ap.create_box("test", "data")
ap.create_frame("test", "data", "sensors", {
    "timestamp": "int64",
    "temperature": "float64",
    "humidity": "float64",
})

# Write some data
table = pa.table({
    "timestamp": [1, 2, 3],
    "temperature": [22.5, 23.1, 22.8],
    "humidity": [45.0, 47.0, 46.0],
})

sink = pa.BufferOutputStream()
writer = pa.ipc.new_stream_writer(sink, table.schema)
writer.write_table(table)
writer.close()

result = ap.write_to_frame("test", "data", "sensors", sink.getvalue().to_pybytes())
print(f"Written {result['rows_written']} rows in {result['cells_written']} cells")

# Query the data
result_bytes = ap.sql("SELECT AVG(temperature) as avg_temp FROM test.data.sensors")
reader = pa.ipc.open_stream(result_bytes)
result = reader.read_all()
print(f"Average temperature: {result.to_pandas()['avg_temp'][0]:.2f}°C")

ap.shutdown()
print("Apiary shutdown complete")
EOF

# Test the installation
python3 ~/apiary-test.py
```

### Multi-Node Mode (Distributed Swarm)

For multi-node deployment, you'll need shared object storage. Options include:

#### Option A: MinIO (Self-Hosted S3)

Install MinIO on one of your Pi devices or a separate server:

```bash
# On MinIO host
wget https://dl.min.io/server/minio/release/linux-arm64/minio
chmod +x minio
sudo mv minio /usr/local/bin/

# Create data directory
mkdir -p ~/minio-data

# Start MinIO (use screen or systemd to keep it running)
minio server ~/minio-data --console-address ":9001"

# Note the access credentials shown on first start
# Create a bucket named "apiary" via the web console at http://<pi-ip>:9001
```

#### Option B: AWS S3

If using AWS S3, configure credentials:

```bash
# Install AWS CLI
pip3 install awscli

# Configure credentials
aws configure
# Enter your AWS Access Key ID, Secret Access Key, and region
```

#### Configure Apiary Nodes

On each Raspberry Pi that will join the swarm:

```bash
# For MinIO
cat > ~/apiary-node.py << 'EOF'
import os
from apiary import Apiary

# Set MinIO credentials
os.environ['AWS_ACCESS_KEY_ID'] = 'minioadmin'  # Change to your MinIO credentials
os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin'
os.environ['AWS_ENDPOINT_URL'] = 'http://192.168.1.100:9000'  # Change to your MinIO server IP

# Initialize with S3 storage
ap = Apiary("production", storage="s3://apiary/data")
ap.start()

print(f"Node started: {ap.status()}")
print(f"Swarm status: {ap.swarm_status()}")

# Keep running (in production, use systemd)
import time
try:
    while True:
        time.sleep(60)
        swarm = ap.swarm_status()
        print(f"Swarm update: {swarm['alive']} nodes alive, {swarm['total_bees']} total bees")
except KeyboardInterrupt:
    ap.shutdown()
    print("Node shutdown complete")
EOF

# For AWS S3
cat > ~/apiary-node.py << 'EOF'
from apiary import Apiary

# AWS credentials should be configured via AWS CLI or environment variables
ap = Apiary("production", storage="s3://my-bucket/apiary")
ap.start()

print(f"Node started: {ap.status()}")
print(f"Swarm status: {ap.swarm_status()}")

import time
try:
    while True:
        time.sleep(60)
        swarm = ap.swarm_status()
        print(f"Swarm update: {swarm['alive']} nodes alive, {swarm['total_bees']} total bees")
except KeyboardInterrupt:
    ap.shutdown()
    print("Node shutdown complete")
EOF
```

## Running as a System Service

For production deployment, use systemd to manage Apiary as a service:

```bash
# Create systemd service file
sudo tee /etc/systemd/system/apiary.service > /dev/null << 'EOF'
[Unit]
Description=Apiary Distributed Data Processing Node
After=network.target

[Service]
Type=simple
User=pi
WorkingDirectory=/home/pi
Environment="PATH=/home/pi/.cargo/bin:/usr/local/bin:/usr/bin:/bin"
Environment="AWS_ACCESS_KEY_ID=minioadmin"
Environment="AWS_SECRET_ACCESS_KEY=minioadmin"
Environment="AWS_ENDPOINT_URL=http://192.168.1.100:9000"
ExecStart=/usr/bin/python3 /home/pi/apiary-node.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd, enable and start the service
sudo systemctl daemon-reload
sudo systemctl enable apiary.service
sudo systemctl start apiary.service

# Check status
sudo systemctl status apiary.service

# View logs
sudo journalctl -u apiary.service -f
```

## Adding a New Node to the Swarm

Adding a new Raspberry Pi to an existing Apiary swarm is simple:

1. **Prepare the new Pi** following the "Operating System Setup" and "Install Dependencies" sections above
2. **Build or install Apiary** using either build option
3. **Configure the node** with the same storage URL as existing nodes
4. **Start the node** - it will automatically discover and join the swarm

```bash
# On the new Pi
cd ~
# Use the same storage configuration as other nodes
python3 apiary-node.py

# Or enable as a service
sudo systemctl enable apiary.service
sudo systemctl start apiary.service
```

The new node will:
- Write its heartbeat to the shared storage
- Be discovered by other nodes within 5-10 seconds
- Start accepting distributed query tasks automatically

**No seed nodes, no configuration changes on existing nodes, no restarts required.**

## Verification

### Verify Single Node

```bash
python3 ~/apiary-test.py
```

Expected output:
```
Apiary running on 4 cores with 3.70GB memory
Written 3 rows in 1 cells
Average temperature: 22.80°C
Apiary shutdown complete
```

### Verify Multi-Node Swarm

From any node in the swarm:

```python
from apiary import Apiary
import os

# Configure storage (adjust for your setup)
os.environ['AWS_ACCESS_KEY_ID'] = 'minioadmin'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin'
os.environ['AWS_ENDPOINT_URL'] = 'http://192.168.1.100:9000'

ap = Apiary("production", storage="s3://apiary/data")
ap.start()

# Check swarm status
swarm = ap.swarm_status()
print(f"Swarm Status:")
print(f"  Alive nodes: {swarm['alive']}")
print(f"  Total bees: {swarm['total_bees']}")
print(f"  Total memory: {swarm['total_memory_gb']:.2f}GB")
print(f"\nNodes:")
for node in swarm['nodes']:
    print(f"  - {node['node_id']}: {node['state']} ({node['cores']} cores, {node['memory_gb']:.2f}GB)")

ap.shutdown()
```

Expected output for a 3-node swarm:
```
Swarm Status:
  Alive nodes: 3
  Total bees: 12
  Total memory: 11.10GB

Nodes:
  - apiary-pi-01: alive (4 cores, 3.70GB)
  - apiary-pi-02: alive (4 cores, 3.70GB)
  - apiary-pi-03: alive (4 cores, 3.70GB)
```

## Performance Tuning

### Memory Configuration

Adjust memory settings for your Pi's RAM capacity:

```python
# For Pi 4 with 2GB RAM
ap = Apiary("production", storage="s3://apiary/data")
# Apiary automatically detects available memory
# Each bee gets ~512MB on a 2GB Pi

# Monitor memory pressure
colony = ap.colony_status()
print(f"Colony temperature: {colony['temperature']:.2f}")
print(f"Regulation: {colony['regulation']}")
```

### Storage Performance

For better I/O performance:

1. **Use USB 3.0 SSD** instead of SD card for data storage
2. **Mount SSD** and configure local cache:

```bash
# Mount USB SSD (assuming /dev/sda1)
sudo mkdir -p /mnt/apiary-cache
sudo mount /dev/sda1 /mnt/apiary-cache
sudo chown pi:pi /mnt/apiary-cache

# Update apiary-node.py to use SSD for cache
# (Apiary will use local disk cache automatically)
```

### Network Configuration

For stable operation:

```bash
# Use static IP (via router DHCP reservation or manual configuration)
# Edit /etc/dhcpcd.conf for static IP
sudo nano /etc/dhcpcd.conf

# Add:
interface eth0
static ip_address=192.168.1.101/24
static routers=192.168.1.1
static domain_name_servers=192.168.1.1 8.8.8.8

# Restart networking
sudo systemctl restart dhcpcd
```

## Monitoring

### System Resources

```bash
# CPU and memory usage
htop

# Disk usage
df -h

# Network activity
sudo iftop
```

### Apiary Logs

```bash
# If running as systemd service
sudo journalctl -u apiary.service -f

# Check for errors
sudo journalctl -u apiary.service --since "1 hour ago" | grep -i error
```

### Swarm Health

Create a monitoring script:

```python
#!/usr/bin/env python3
import os
from apiary import Apiary
import time

os.environ['AWS_ACCESS_KEY_ID'] = 'minioadmin'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin'
os.environ['AWS_ENDPOINT_URL'] = 'http://192.168.1.100:9000'

ap = Apiary("production", storage="s3://apiary/data")
ap.start()

try:
    while True:
        swarm = ap.swarm_status()
        colony = ap.colony_status()
        
        print(f"\n=== Swarm Health Report ===")
        print(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Alive nodes: {swarm['alive']}")
        print(f"Suspect nodes: {swarm['suspect']}")
        print(f"Dead nodes: {swarm['dead']}")
        print(f"Colony temperature: {colony['temperature']:.2f}")
        print(f"Regulation: {colony['regulation']}")
        
        if swarm['dead'] > 0:
            print("WARNING: Dead nodes detected!")
            for node in swarm['nodes']:
                if node['state'] == 'dead':
                    print(f"  Dead: {node['node_id']}")
        
        time.sleep(30)
except KeyboardInterrupt:
    ap.shutdown()
```

## Troubleshooting

### Node Not Joining Swarm

```bash
# Check network connectivity to storage
# For MinIO
curl http://192.168.1.100:9000

# Check credentials
python3 -c "
import os
os.environ['AWS_ACCESS_KEY_ID'] = 'minioadmin'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin'
os.environ['AWS_ENDPOINT_URL'] = 'http://192.168.1.100:9000'
import boto3
s3 = boto3.client('s3')
print(s3.list_buckets())
"
```

### Out of Memory Errors

```bash
# Check available memory
free -h

# Monitor memory during operation
watch -n 1 free -h

# Reduce workload or add swap
sudo dphys-swapfile swapoff
sudo nano /etc/dphys-swapfile
# Set CONF_SWAPSIZE=2048 (2GB)
sudo dphys-swapfile setup
sudo dphys-swapfile swapon
```

### Build Failures

```bash
# If cargo build fails with linking errors
# Install missing dependencies
sudo apt install -y libssl-dev pkg-config

# If running out of memory during build
# Use swap or build on a more powerful machine and cross-compile
```

### Node Shows as Dead

```bash
# Check if node is actually running
sudo systemctl status apiary.service

# Check heartbeat is being written
# For MinIO/S3, check the _heartbeats/ directory in your bucket

# Check system time is synchronized
sudo timedatectl status
# Enable NTP if disabled
sudo timedatectl set-ntp true
```

## Security Considerations

### Network Security

```bash
# Use firewall to restrict access
sudo apt install -y ufw
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow ssh
sudo ufw allow from 192.168.1.0/24  # Allow local network
sudo ufw enable
```

### Storage Security

- Use strong credentials for MinIO or S3
- Enable HTTPS for MinIO:
  ```bash
  # Generate self-signed certificate
  mkdir -p ~/.minio/certs
  openssl req -x509 -newkey rsa:4096 -keyout ~/.minio/certs/private.key \
    -out ~/.minio/certs/public.crt -days 365 -nodes
  ```
- Use IAM roles and least-privilege policies for AWS S3

### Data Security

- Apiary stores data in Parquet format (not encrypted by default)
- Enable server-side encryption in S3/MinIO for data at rest
- Use VPC endpoints or VPN for communication with cloud storage

## Next Steps

- See [Python SDK Reference](python-sdk.md) for API details
- See [SQL Reference](sql-reference.md) for query syntax
- See [Architecture Summary](architecture-summary.md) for system design
- See [Cloud Container Deployment](deployment-containers.md) for Docker/Kubernetes deployments

## Support

For issues or questions:
- GitHub Issues: https://github.com/ApiaryData/apiary/issues
- Documentation: https://github.com/ApiaryData/apiary/tree/main/docs
