    [CmdletBinding()]
    param (
        [Parameter()][int]$AgentCount,
        [Parameter()][ValidateSet("Plain", "PBAC")][string]$Mode,
        [Parameter(Mandatory = $false)][Switch]$BenchmarkProducer,
        [Parameter(Mandatory = $false)][Switch]$BenchmarkConsumer,
        [Parameter()][int]$DurationInSeconds = 600
    )

    begin {
        $logMessageQueryIntervalSeconds = 60
    }

    process {
        Push-Location ..\tf

        terraform destroy -var='client_types=producer' -var='client_types=consumer' -var="enable_pbac=true" -target='module.load_generator' -target='module.kafka' -auto-approve
        terraform apply -var='client_types=producer' -var="client_count=$AgentCount" -var="enable_pbac=$(($Mode -eq 'PBAC') + 0)" -var="benchmark_msgs_to_send=2147483647" -auto-approve

        Pop-Location

        Start-Sleep -Seconds 180

        $beginTs = Get-Date -Format O

        gcloud compute scp THE_FILE controller:THE_FILE

        $controllerShellCommand = "for i in {0..$( $AgentCount - 1 )}; do cat THE_FILE | ncat producer-`$((i)) 8080; done;"

        gcloud compute ssh --zone "us-central1-f" --project "pbac-in-pubsub" "controller" --command $controllerShellCommand

        for($i=0; $i -lt ($DurationInSeconds / $logMessageQueryIntervalSeconds); $i++)
        {
            Start-Sleep -Seconds $logMessageQueryIntervalSeconds

            $logMessageQueryStartTimestamp = [System.DateTime]::Now.Subtract([System.TimeSpan]::FromSeconds($logMessageQueryIntervalSeconds)).ToString("O")
            $filterString = ' jsonPayload.\"cos.googleapis.com/container_name\" != \"stackdriver-logging-agent\"' +
                    ' jsonPayload.\"cos.googleapis.com/container_name\" != kafka' +
                    ' jsonPayload.\"cos.googleapis.com/container_name\" != pbac' +
                    " timestamp > \`"$logMessageQueryStartTimestamp\`""

            gcloud logging read $filterString --bucket=benchmark-results --view _AllLogs --location us-central1 --limit 1 --format 'get(jsonPayload.message)'
        }

        Write-Output "Ending producer run after $DurationInSeconds seconds."

        $endTs = Get-Date -Format O

        "Produce;$Mode;$AgentCount;$beginTs;$endTs" >> BenchmarkRuns.csv

        if ($BenchmarkConsumer) {
            Push-Location ..\tf

            terraform destroy -var='client_types=producer' -var='client_types=consumer' -var='enable_pbac=1' -target='module.load_generator' -auto-approve
            terraform apply -var='client_types=consumer' -var="client_count=$AgentCount" -var='enable_pbac=1' -target='module.load_generator' -auto-approve

            Pop-Location

            Start-Sleep -Seconds 60

            $beginTs = Get-Date -Format O

            $startConsumptionCommand = "for i in {0..$( $AgentCount - 1 )}; do echo 'GO!' | ncat consumer-`$((i)) 8080; done;"

            gcloud compute ssh --zone "us-central1-f" --project "pbac-in-pubsub" "controller" --command $startConsumptionCommand

            for($i=0; $i -lt ($DurationInSeconds / $logMessageQueryIntervalSeconds); $i++)
            {
                Start-Sleep -Seconds $logMessageQueryIntervalSeconds

                $logMessageQueryStartTimestamp = [System.DateTime]::Now.Subtract([System.TimeSpan]::FromSeconds($logMessageQueryIntervalSeconds)).ToString("O")
                $filterString = ' jsonPayload.\"cos.googleapis.com/container_name\" != \"stackdriver-logging-agent\"' +
                        " timestamp > \`"$logMessageQueryStartTimestamp\`"" # + 'jsonPayload.message =~ \"^\d\"'

                gcloud logging read $filterString --bucket=benchmark-results --view _AllLogs --location us-central1 --limit 1 --format 'get(jsonPayload.message)'
            }

            Write-Output "Ending consumer run after $DurationInSeconds seconds."

            $endTs = Get-Date -Format O

            "Consume;$Mode;$AgentCount;$beginTs;$endTs" >> BenchmarkRuns.csv
        }

        Push-Location ..\tf
        terraform destroy -var='client_types=producer' -var='client_types=consumer' -var="enable_pbac=true" -target='module.load_generator' -target='module.kafka' -auto-approve
        Pop-Location
    }