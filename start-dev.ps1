$root = $PSScriptRoot

$dashboard  = "$root\src\Streamly.Dashboard"
$publisher  = "$root\examples\Streamly.FakePublisher"
$subscriber = "$root\examples\Streamly.FakeSubscriber"

# --- Service launcher helpers ---

function Start-Dashboard {
    Write-Host "  Starting Dashboard..." -ForegroundColor Cyan
    Start-Process powershell -ArgumentList "-NoExit", "-Command", `
        "`$host.UI.RawUI.WindowTitle = 'Streamly Dashboard'; cd '$dashboard'; dotnet run --no-build --launch-profile http"
}

function Start-PublisherA {
    Write-Host "  Starting Publisher A..." -ForegroundColor Green
    Start-Process powershell -ArgumentList "-NoExit", "-Command", `
        "`$host.UI.RawUI.WindowTitle = 'Fake Publisher A'; cd '$publisher'; dotnet run --no-build --launch-profile Publisher-InstanceA"
}

function Start-PublisherB {
    Write-Host "  Starting Publisher B..." -ForegroundColor Green
    Start-Process powershell -ArgumentList "-NoExit", "-Command", `
        "`$host.UI.RawUI.WindowTitle = 'Fake Publisher B'; cd '$publisher'; dotnet run --no-build --launch-profile Publisher-InstanceB"
}

function Start-SubscriberA {
    Write-Host "  Starting Subscriber A..." -ForegroundColor Magenta
    Start-Process powershell -ArgumentList "-NoExit", "-Command", `
        "`$host.UI.RawUI.WindowTitle = 'Streamly Subscriber A'; cd '$subscriber'; dotnet run --no-build --launch-profile 'FakeSubscriber A'"
}

function Start-SubscriberB {
    Write-Host "  Starting Subscriber B..." -ForegroundColor Magenta
    Start-Process powershell -ArgumentList "-NoExit", "-Command", `
        "`$host.UI.RawUI.WindowTitle = 'Streamly Subscriber B'; cd '$subscriber'; dotnet run --no-build --launch-profile 'FakeSubscriber B'"
}

# --- Menu ---

Write-Host ""
Write-Host "===============================" -ForegroundColor DarkCyan
Write-Host "   Streamly Dev Launcher" -ForegroundColor Cyan
Write-Host "===============================" -ForegroundColor DarkCyan
Write-Host ""
Write-Host "  1  All (staged: Dashboard+PubA -> SubA -> PubB+SubB)" -ForegroundColor White
Write-Host "  2  Dashboard only" -ForegroundColor White
Write-Host "  3  Publisher A only" -ForegroundColor White
Write-Host "  4  Publisher B only" -ForegroundColor White
Write-Host "  5  Subscriber A only" -ForegroundColor White
Write-Host "  6  Subscriber B only" -ForegroundColor White
Write-Host "  7  Publishers only (A + B)" -ForegroundColor White
Write-Host "  8  Subscribers only (A + B)" -ForegroundColor White
Write-Host "  9  Dashboard + Publisher A" -ForegroundColor White
Write-Host "  10 Dashboard + Full stack A (PubA + SubA)" -ForegroundColor White
Write-Host ""

$choice = Read-Host "Enter your choice"

# --- Build ---

Write-Host "`nBuilding all projects..." -ForegroundColor Yellow
dotnet build "$root\Streamly.sln" --configuration Debug
if ($LASTEXITCODE -ne 0) {
    Write-Host "Build failed. Aborting." -ForegroundColor Red
    exit 1
}
Write-Host ""

# --- Dispatch ---

switch ($choice) {

    "1" {
        Write-Host "Starting all services (staged)..." -ForegroundColor Yellow
        Start-Dashboard
        Start-PublisherA
        Write-Host "  Waiting 5s before Subscriber A..." -ForegroundColor DarkYellow
        Start-Sleep -Seconds 5
        Start-SubscriberA
        Write-Host "  Waiting 5s before Publisher B + Subscriber B..." -ForegroundColor DarkYellow
        Start-Sleep -Seconds 5
        Start-PublisherB
        Start-SubscriberB
    }

    "2" { Start-Dashboard }

    "3" { Start-PublisherA }

    "4" { Start-PublisherB }

    "5" { Start-SubscriberA }

    "6" { Start-SubscriberB }

    "7" {
        Start-PublisherA
        Start-PublisherB
    }

    "8" {
        Start-SubscriberA
        Start-SubscriberB
    }

    "9" {
        Start-Dashboard
        Start-PublisherA
    }

    "10" {
        Start-Dashboard
        Start-PublisherA
        Write-Host "  Waiting 5s before Subscriber A..." -ForegroundColor DarkYellow
        Start-Sleep -Seconds 5
        Start-SubscriberA
    }

    default {
        Write-Host "Invalid choice '$choice'. Aborting." -ForegroundColor Red
        exit 1
    }
}

Write-Host "`nDone." -ForegroundColor Yellow