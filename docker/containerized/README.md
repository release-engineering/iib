# IIB Containerized Workflow Development Environment

This directory contains configuration for running IIB in containerized mode, where build operations are executed in an external Konflux cluster instead of locally in the worker.

## Architecture Overview

In the containerized workflow:

1. **IIB Worker** receives a request (e.g., remove operators)
2. Worker clones the Git repository containing the catalog
3. Worker makes changes to the catalog locally
4. Worker commits and pushes changes to GitLab (either to a branch or creates an MR)
5. GitLab push triggers a **Konflux PipelineRun** in the external cluster
6. Worker monitors the PipelineRun status via Kubernetes API
7. When the PipelineRun completes, worker copies the built image from Konflux to IIB registry
8. Worker updates the index.db artifact and completes the request

## Prerequisites

1. **Konflux Cluster Access**
   - A Konflux dev cluster with pipelines configured
   - Service account with permissions to read/list PipelineRuns
   - Cluster CA certificate
   - Cluster API URL

2. **GitLab Access**
   - GitLab repositories for catalog storage
   - GitLab access tokens with write permissions

3. **Container Runtime**
   - Podman installed and configured
   - podman-compose installed

## Setup Instructions

### 1. Get Konflux Cluster Credentials

#### Get the Cluster API URL

```bash
kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}'
```

#### Create a Service Account

```bash
# Set your namespace
NAMESPACE="your-namespace"

# Create service account
kubectl create serviceaccount iib-worker -n $NAMESPACE

# Create role with PipelineRun permissions
cat <<EOF | kubectl apply -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: iib-pipelinerun-reader
  namespace: $NAMESPACE
rules:
- apiGroups: ["tekton.dev"]
  resources: ["pipelineruns"]
  verbs: ["get", "list", "watch"]
EOF

# Create role binding
kubectl create rolebinding iib-worker-pipelinerun-reader \
  --role=iib-pipelinerun-reader \
  --serviceaccount=$NAMESPACE:iib-worker \
  -n $NAMESPACE
```

#### Generate a Token

```bash
# Generate a token valid for 30 days (720 hours)
kubectl create token iib-worker -n $NAMESPACE --duration=720h
```

Save this token - you'll need it for the configuration.

#### Get the Cluster CA Certificate

```bash
kubectl config view --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' | base64 -d > konflux-ca.crt
```

This will save the CA certificate to `konflux-ca.crt` in the current directory.

### 2. Configure Environment Variables

1. Copy the template file:
   ```bash
   cp .env.containerized.template .env.containerized
   ```

2. Edit `.env.containerized` and fill in the required values:

   ```bash
   # Konflux Cluster Configuration
   IIB_KONFLUX_CLUSTER_URL=https://api.konflux-dev.example.com:6443
   IIB_KONFLUX_CLUSTER_TOKEN=eyJhbGc...  # Token from above
   IIB_KONFLUX_CLUSTER_CA_CERT=/etc/iib/konflux-ca.crt
   IIB_KONFLUX_NAMESPACE=your-namespace

   # GitLab Configuration
   IIB_INDEX_CONFIGS_GITLAB_TOKENS_MAP='{"https://gitlab.example.com/catalogs/v4.19": GITLAB_TOKEN_V419:glpat-xxxxxxxxxxxxx"}'

   # Registry Configuration
   IIB_REGISTRY=registry:8443
   IIB_IMAGE_PUSH_TEMPLATE={registry}/iib-build:{request_id}

   # Index DB Artifact Configuration
   IIB_INDEX_DB_ARTIFACT_REGISTRY=quay.io/your-org
   IIB_INDEX_DB_IMAGESTREAM_REGISTRY=image-registry.openshift-image-registry.svc:5000
   ```

### 3. Place the Konflux CA Certificate

Copy the CA certificate you downloaded to the correct location:

```bash
cp konflux-ca.crt docker/containerized/konflux-ca.crt
```

### 4. Start the Development Environment

```bash
# Start all services
podman-compose -f podman-compose-containerized.yml up -d

# View logs
podman-compose -f podman-compose-containerized.yml logs -f iib-worker-containerized

# Stop all services
podman-compose -f podman-compose-containerized.yml down
```

## Testing the Setup

### 1. Verify Services are Running

```bash
podman-compose -f podman-compose-containerized.yml ps
```

You should see:
- `iib-api` (running)
- `iib-worker-containerized` (running)
- `db` (running)
- `rabbitmq` (running)
- `registry` (running)
- `memcached` (running)
- `message-broker` (running)
- `minica` (exited 0)

### 2. Check Worker Logs

```bash
podman-compose -f podman-compose-containerized.yml logs iib-worker-containerized
```

Look for:
- "Configuring Kubernetes client for cross-cluster access to https://..."
- No errors about missing Konflux configuration

### 3. Submit a Test Request

```bash
# Using the IIB API
curl -X POST http://localhost:8080/api/v1/builds/rm \
  -H "Content-Type: application/json" \
  -d '{
    "from_index": "registry.example.com/catalog:v4.19",
    "operators": ["test-operator"],
    "index_to_gitlab_push_map": {
      "registry.example.com/catalog:v4.19": "https://gitlab.example.com/catalogs/v4.19"
    },
    "overwrite_from_index": false
  }'
```

### 4. Monitor the Request

Watch the worker logs to see:
1. Cloning the Git repository
2. Removing operators from the catalog
3. Committing and pushing to GitLab
4. Waiting for Konflux pipeline
5. Pipeline completion
6. Copying built image to IIB registry
7. Request completion

## Troubleshooting

### Worker Can't Connect to Konflux Cluster

**Symptoms:** Error messages about Kubernetes client initialization

**Solution:**
1. Verify the cluster URL is correct and accessible
2. Check that the token is valid (not expired)
3. Ensure the CA certificate is correct
4. Test connection manually:
   ```bash
   kubectl --server=<CLUSTER_URL> --token=<TOKEN> \
     --certificate-authority=docker/containerized/konflux-ca.crt \
     get pipelineruns -n <NAMESPACE>
   ```

### Permission Denied Errors

**Symptoms:** Kubernetes API errors about permissions

**Solution:**
1. Verify the service account has the correct role binding
2. Check that the role includes `get`, `list`, and `watch` verbs for `pipelineruns`
3. Ensure you're using the correct namespace

### GitLab Authentication Errors

**Symptoms:** Errors cloning or pushing to GitLab

**Solution:**
1. Verify the GitLab token has correct permissions (read_repository, write_repository)
2. Check the token hasn't expired
3. Ensure the repository URL in `index_to_gitlab_push_map` is correct
4. Test the token manually:
   ```bash
   git clone https://oauth2:<TOKEN>@gitlab.example.com/catalogs/v4.19.git
   ```

### Pipeline Timeout

**Symptoms:** "Timeout waiting for pipelinerun to complete"

**Solution:**
1. Increase `IIB_KONFLUX_PIPELINE_TIMEOUT` in `.env.containerized`
2. Check the Konflux pipeline logs to see why it's taking long
3. Verify the pipeline isn't stuck or failing silently

## Configuration Reference

### Environment Variables

All environment variables are documented in `.env.containerized.template`.

### Worker Configuration

The worker configuration is in `docker/containerized/worker_config.py`. This file:
- Reads environment variables from `.env.containerized`
- Extends the base `DevelopmentConfig`
- Includes the containerized task modules
- Validates required configuration on startup

## Differences from Traditional Workflow

| Aspect | Traditional Workflow | Containerized Workflow |
|--------|---------------------|------------------------|
| Build Location | Local in worker container | External Konflux cluster |
| Worker Privileges | Privileged (for building) | Unprivileged |
| Container Storage | Requires large volumes | Minimal storage needed |
| Git Operations | Optional | Required |
| External Dependencies | Local tools (buildah, podman) | Konflux cluster, GitLab |
| Scalability | Limited by worker resources | Limited by Konflux capacity |

## Additional Resources

- [IIB Documentation](../../docs/)
- [Konflux Documentation](https://konflux-ci.dev/)
- [GitLab API Documentation](https://docs.gitlab.com/ee/api/)
- [Tekton PipelineRuns](https://tekton.dev/docs/pipelines/pipelineruns/)

## Contributing

If you encounter issues or have improvements:

1. Check existing issues and documentation
2. Test your changes locally
3. Submit a pull request with clear description
