[CmdletBinding()]
param (
    [Parameter()][int]$ProducerCount,
    [Parameter()][string]$ProducerScript,
    [Parameter()][string]$PayloadFile,
    [Parameter()][int]$NumRecords = 10000000
)
    
begin {
        
}
    
process {
    $jobs = 1..$ProducerCount | ForEach-Object {
        &$ProducerScript --num-records $NumRecords --payload-file $PayloadFile --throughput -1 --topic 'producer-bench' --producer-props 'bootstrap.servers=localhost:9092'&
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
    } | Export-Csv -Path "Results.$ProducerCount.csv"
}

    
end {
        
}