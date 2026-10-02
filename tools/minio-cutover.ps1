param(
    [Parameter(Mandatory=$true)]
    [ValidateSet('Prepare','Run','Status','Validate')]
    [string]$Stage
)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$ComposeArgs = @('-f','docker-compose.yml','-f','docker-compose.airflow.yml',
    '-f','docker-compose.monitoring.yml','-f','docker-compose.storage.yml',
    '-f','docker-compose.cutover.yml')
function Invoke-Compose {
    & docker compose @ComposeArgs @args
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose failed (exit $LASTEXITCODE). Stop here and share the error." }
}
function Assert-Idle {
    $guard = @'
from airflow.models import DagRun, DagModel
from airflow.utils.session import create_session
with create_session() as session:
    names = ['commerce_lakehouse_pipeline', 'commerce_lakehouse_minio']
    for name in names:
        model = session.query(DagModel).filter(DagModel.dag_id == name).first()
        if name == names[0] and (model is None or not model.is_paused):
            raise RuntimeError('Original DAG must be paused.')
    active = session.query(DagRun).filter(DagRun.dag_id.in_(names), DagRun.state.in_(['running','queued'])).all()
    if active:
        raise RuntimeError('Active runs must finish before cutover: ' + ', '.join(r.run_id for r in active))
print('PASS: no active runs in either pipeline.')
'@
    $guard | & docker compose @ComposeArgs exec -T airflow-scheduler python -
    if ($LASTEXITCODE -ne 0) { throw 'An active run or unavailable scheduler blocked this step. Stop here.' }
}
if ($Stage -eq 'Prepare') {
    # The existing scheduler must be running. Pause before changing its environment.
    Invoke-Compose exec -T airflow-scheduler airflow dags pause commerce_lakehouse_pipeline
    Assert-Idle
    Invoke-Compose up -d postgres kafka connect minio airflow-db airflow-scheduler airflow-webserver
    $parse = @'
from airflow.models import DagBag
bag = DagBag('/opt/airflow/dags/commerce_lakehouse_minio.py', include_examples=False)
if bag.import_errors:
    raise RuntimeError(str(bag.import_errors))
assert 'commerce_lakehouse_minio' in bag.dags, 'New DAG missing'
print('PASS: MinIO DAG imports successfully.')
'@
    $parse | & docker compose @ComposeArgs exec -T airflow-scheduler python -
    if ($LASTEXITCODE -ne 0) { throw 'MinIO DAG import failed. Stop here.' }
    Invoke-Compose run --rm --no-deps minio-runner /opt/spark/apps/minio/prepare.py
    Write-Host 'PREPARE finished. The original DAG remains paused. No new DAG run was triggered.'
    Write-Host 'Next: .\tools\minio-cutover.ps1 Run'
}
elseif ($Stage -eq 'Run') {
    Assert-Idle
    if (-not (Test-Path 'lakehouse/cutover/minio_start_v1.json')) { throw 'Run Prepare first.' }
    Invoke-Compose exec -T airflow-scheduler airflow dags unpause commerce_lakehouse_minio
    $RunId = 'minio_validation_' + (Get-Date -Format 'yyyyMMdd_HHmmss')
    Invoke-Compose exec -T airflow-scheduler airflow dags trigger -r $RunId commerce_lakehouse_minio
    Write-Host "Triggered $RunId. Monitor at http://localhost:8081 or run the Status stage."
}
elseif ($Stage -eq 'Validate') {
    Assert-Idle
    Invoke-Compose run --rm --no-deps minio-runner /opt/spark/apps/minio/validate.py
}
else {
    Invoke-Compose exec -T airflow-scheduler airflow dags list-runs -d commerce_lakehouse_minio
}
