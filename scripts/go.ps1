# 執行 go 指令：本機有 Go 就直接用，沒有就透過 golang 映像檔執行。
# 用法：.\scripts\go.ps1 test ./...
#
# 設定 $env:GO_IN_DOCKER = '1' 可強制用容器執行。race detector（go test -race）需要 cgo，
# 也就是需要 C 編譯器；Windows 本機通常沒有，容器裡有：
#   $env:GO_IN_DOCKER='1'; .\scripts\go.ps1 test -race ./...
$root = Split-Path -Parent $PSScriptRoot

if (-not $env:GO_IN_DOCKER -and (Get-Command go -ErrorAction SilentlyContinue)) {
    if (-not $env:DATABASE_URL) { $env:DATABASE_URL = 'postgres://tester:tester@localhost:5440/tester' }
    if (-not $env:AMQP_URL) { $env:AMQP_URL = 'amqp://tester:tester@localhost:5673/' }
    Push-Location $root
    try { & go @args } finally { Pop-Location }
    exit $LASTEXITCODE
}

# 容器內要連 compose 的 postgres，所以掛到同一個網路（compose 沒起來時就不掛）
$net = @()
docker network inspect tester-receiver_default *> $null
if ($LASTEXITCODE -eq 0) { $net = @('--network', 'tester-receiver_default') }

docker run --rm @net `
    -v "${root}:/src" -w /src `
    -v tr-gomod:/go/pkg/mod -v tr-gocache:/root/.cache/go-build `
    -e DATABASE_URL=postgres://tester:tester@postgres:5432/tester `
    -e AMQP_URL=amqp://tester:tester@rabbitmq:5672/ `
    golang:1.26 go @args
exit $LASTEXITCODE
