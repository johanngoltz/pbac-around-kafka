[CmdletBinding()]
param (
    [Parameter()][int]$AgentCount,
    [Parameter()][string]$ScriptDirectory,
    [Parameter()][string]$PayloadFile,
    [Parameter()][int]$NumRecords = 10000000,
    [Parameter()][ValidateSet("Plain", "PBAC")][string]$Mode,
    [Parameter(Mandatory = $false)][Switch]$BenchmarkProducer,
    [Parameter(Mandatory = $false)][Switch]$BenchmarkConsumer
)
    
begin {
    $directorys = @{"PBAC" = "benchmark"; "Plain" = "regular-kafka" }

    Push-Location (($Mode -eq "PBAC") ? '..\resources\regular-kafka' : '..\resources\benchmark')
    docker-compose down
    Start-Sleep 10
    Pop-Location

    Push-Location (Join-Path '..\resources' $directorys[$Mode])
    docker-compose up -d
    Start-Sleep 10
    Pop-Location

    $env:KAFKA_OPTS = '-Duser.country=GB -Duser.language=en'
    'check.crcs=false' > 'NoCheckCrcs.config'
}
    
process {
    if ($BenchmarkProducer) {
        $jobs = 1..$AgentCount | ForEach-Object {
            &(Join-Path $ScriptDirectory 'kafka-producer-perf-test.bat') --num-records $NumRecords --payload-file $PayloadFile --throughput -1 --topic 'producer-bench' --producer-props 'bootstrap.servers=localhost:9092'&
        }
        $jobs
        Wait-Job $jobs
        $jobs | ForEach-Object {
            $timings = ((Receive-Job -Keep $PSItem 2>$null # discard stderr, should contain only slf4j classpath warnings
                    | Tee-Object -FilePath "$($PSItem.Id).log"
                    | Select-Object -Skip 2
                    | Select-String -Pattern '(?<RecordsPerSecond>[\d,.]+) records/sec.*?(?<AvgLatency>[\d,.]+) ms avg latency'
                ).Matches
            ) | Foreach-Object { @{
                    'RecordsPerSecond' = [Math]::Round([Decimal]::parse($_.Groups['RecordsPerSecond']));
                    'AvgLatency'       = [Math]::Round([Decimal]::parse($_.Groups['AvgLatency']));
                } }
            $result = for ($i = 1; $i -lt $timings.Count; $i++) {
                @{'producer' = $PSItem.Id; 'elapsed' = $i * 5 } + $timings[$i - 1]
            }
            $result
        } | Export-Csv -Path "$Mode\Results.Produce.$AgentCount.csv"
    }

    if ($BenchmarkConsumer) {
        $jobs = 1..$AgentCount | Foreach-Object {
            $NumMessagesToConsume = [Math]::Floor($NumRecords * 0.9)
            &(Join-Path $ScriptDirectory \kafka-consumer-perf-test.bat) --topic 'producer-bench' --bootstrap-server 'localhost:9092' --timeout 60_000 --consumer.config 'NoCheckCrcs.config' --messages $NumMessagesToConsume --show-detailed-stats &
        }
        $jobs
        Wait-Job $jobs
        $jobs | Foreach-Object {
            $Job = $PSItem
            Receive-Job -Keep $Job 2>$null
            | ConvertFrom-Csv
            | Foreach-Object { $PSItem | Add-Member -MemberType NoteProperty -Name 'Producer' -Value $Job.Id -PassThru }
        } | Export-Csv -Path "$Mode\Results.Consume.$AgentCount.csv"
    }
}

    
end {
        
}