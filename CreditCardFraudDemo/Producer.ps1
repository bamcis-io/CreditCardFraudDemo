Param(
    [Parameter()]
    [System.String]$ZipPath = "d:\users\$env:Username\source\repos\CreditCardFraudDemo\CreditCardFraudDemo\creditcardfraud.zip"
)

$APIGatewayId = "" # Paste your id for APIG here
$TempPath = "$env:SYSTEMDRIVE\temp\ccfraud"
$Url = " https://$APIGatewayId.execute-api.us-east-1.amazonaws.com/stream/cc-transactions-delivery-stream"


try {
    Expand-Archive -Path $ZipPath -DestinationPath $TempPath -Force
    $Path = Get-ChildItem -Path $TempPath | Select-Object -First 1 -ExpandProperty FullName

    Write-Host "Importing Data"
    $Data = Import-Csv -Path $Path

    Write-Host "Iterating Data"

    $Counter = 0
    
    foreach ($Row in $Data) {
        $Row.PSObject.Properties.Remove("Class")
        $Vals = ($Row.PSObject.Properties | Select-Object -ExpandProperty Value)
        $Data = @{"transactionDetails" = $Vals}
        $Json = ConvertTo-Json -InputObject $Data

        [Microsoft.PowerShell.Commands.WebResponseObject]$Response = Invoke-WebRequest -Uri $Url -Method Post -Body $Json -ContentType "application/json"

	    if ($Response.StatusCode -ne 201)
	    {
		    Write-Warning -Message "Received response with status code $($Response.StatusCode)"
	    }
        else 
        {      
            $Counter++
        }

        if ($Counter % 10 -eq 0)
        {
            Write-Host $Counter
        }
    }
}
finally {
    Remove-Item -Path $TempPath -Recurse -Force -ErrorAction SilentlyContinue
}