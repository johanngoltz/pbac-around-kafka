    [CmdletBinding()]
    param (
        [Parameter()][int]$AgentCount,
        [Parameter()][ValidateSet("Plain", "PBAC")][string]$Mode,
        [Parameter(Mandatory = $false)][Switch]$BenchmarkProducer,
        [Parameter(Mandatory = $false)][Switch]$BenchmarkConsumer,
        [Parameter()][int]$DurationInSeconds = 600
    )

    process {
        if ($BenchmarkProducer)
        {
            Push-Location ..\tf

            terraform destroy -var='client_types=producer' -var="enable_pbac=true" -target='module.load_generator' -target='module.kafka' -auto-approve
            terraform apply -var='client_types=producer' -var="client_count=$AgentCount" -var="enable_pbac=$(($Mode -eq 'PBAC') + 0)" -var="benchmark_msgs_to_send=2147483647" -auto-approve

            Pop-Location

            Start-Sleep -Seconds 180

            $beginTs = Get-Date -Format O

            gcloud compute scp THE_FILE controller:THE_FILE

            $controllerShellCommand = "for i in {0..$( $AgentCount - 1 )}; do cat THE_FILE | ncat producer-`$((i)) 8080; done;"

            gcloud compute ssh --zone "us-central1-f" --project "pbac-in-pubsub" "controller" --command $controllerShellCommand

            $logMessageQueryIntervalSeconds = 60
            for($i=0; $i -lt ($DurationInSeconds / $logMessageQueryIntervalSeconds); $i++)
            {
                Start-Sleep -Seconds $logMessageQueryIntervalSeconds

                $logMessageQueryStartTimestamp = [System.DateTime]::Now.Subtract([System.TimeSpan]::FromSeconds($logMessageQueryIntervalSeconds)).ToString("O")
                $filterString = 'jsonPayload.message =~ \"^\d\"' +
                        ' jsonPayload.\"cos.googleapis.com/container_name\" != \"stackdriver-logging-agent\"' +
                        " timestamp > \`"$logMessageQueryStartTimestamp\`""

                gcloud logging read $filterString --bucket=benchmark-results --view _AllLogs --location us-central1 --limit 1 --format 'get(jsonPayload.message)'
            }

            Write-Output "Ending run after $DurationInSeconds seconds."

            $endTs = Get-Date -Format O

            "Produce;$Mode;$AgentCount;$beginTs;$endTs" >> BenchmarkRuns.csv

            terraform destroy -target='module.load_generator' -target='module.kafka' -auto-approve
        }

    }