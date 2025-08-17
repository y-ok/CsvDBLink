# Set Up Oracle 19c (Docker) and Run the FlexDBLink Sample

## Build the Oracle 19c Docker Image (19.3.0-EE)

```bash
# Get the repository
git clone https://github.com/oracle/docker-images.git
cd docker-images/OracleDatabase/SingleInstance/dockerfiles

# Place the installer ZIP (do NOT extract)
# Download LINUX.X64_193000_db_home.zip from Oracle and put it under 19.3.0/
# Example: 
cp ~/Downloads/LINUX.X64_193000_db_home.zip 19.3.0/

# Build the image (Enterprise Edition)
chmod +x buildContainerImage.sh
./buildContainerImage.sh -v 19.3.0 -e

# Verify the result (19.3.0-ee should be listed)
docker images | grep 'oracle/database'
```

## Start/Stop the Container and View Logs

```bash
# Start
docker compose up -d

# Wait until it becomes healthy
docker ps

# Tail logs
docker logs -f oracle19c

# Stop
docker compose down

# Clean removal (also delete the volume)
docker compose down -v
```

## Deploy FlexDBLink into the script Folder

```bash
cd <repo-root>

# Build from source and place artifacts (skip tests entirely)
mvn clean package -Dmaven.test.skip=true
cd target
unzip FlexDBLink-distribution.zip

cp -pr FlexDBLink <repo-root>/script/.

# Verify placement
ls -1 <repo-root>/script/FlexDBLink
# conf/
# flexdblink.jar
```

## Update data-path (application.yml)

**Location**: `script/FlexDBLink/conf/application.yml`

### Method: hard-code an absolute path

Set an **absolute path** to `data-path`.

```yaml
data-path: /absolute/path/to/your/<repo-root>/script/data
```

## Folder Layout

```
script/
├─ data/
│  ├─ load/
│  │  ├─ pre/DB1/{*.csv,files/*}
│  │  └─ COMMON/DB1/{*.csv,files/*}
│  └─ dump/COMMON/DB1/{*.csv,files/*}
└─ FlexDBLink/
   ├─ conf/application.yml
   └─ flexdblink.jar
```

## Run with the Sample Data

```bash
cd script/FlexDBLink

# Initial load (uses "pre")
java -Dspring.config.additional-location=file:conf/ -jar flexdblink.jar --load

# Scenario load (COMMON): delete duplicates + INSERT only
java -Dspring.config.additional-location=file:conf/ -jar flexdblink.jar --load COMMON

# Dump (COMMON): outputs CSV and LOB files
java -Dspring.config.additional-location=file:conf/ -jar flexdblink.jar --dump COMMON
```
