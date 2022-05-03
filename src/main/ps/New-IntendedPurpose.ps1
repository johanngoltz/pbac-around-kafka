[CmdletBinding()]
param (
    [Parameter()]
    [string]
    $UserId,
    [Parameter()]
    [string]
    $TopicName,
    [Parameter()]
    [string]
    $IntendedPurpose,
    [Parameter()]
    [string]
    $Condition
)

$iso8601Ts = Get-Date -Format FileDateTimeUniversal
$json = ConvertTo-Json -Compress @{userId = $UserId; topic = $TopicName; purpose = $IntendedPurpose; condition = $Condition}
"$TopicName`$$UserId`$$iso8601Ts`t$json" | ./kafka-console-producer --topic 'reservations' --bootstrap-server localhost:9092 --property parse.key=true