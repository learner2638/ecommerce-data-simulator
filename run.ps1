Write-Host "Building Docker image..."

docker build -t data-sim .

Write-Host "Stopping old container..."
docker rm -f data-sim-test 2>$null

Write-Host "Starting service..."

docker run `
--name data-sim-test `
--rm `
-p 8000:8000 `
-v ${PSScriptRoot}\out:/app/out `
data-sim

Start-Process "http://127.0.0.1:8000/ui/"