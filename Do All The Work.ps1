#Params...
$WhereAmI = $MyInvocation.MyCommand.Path.Replace($MyInvocation.MyCommand.Name,"")

$DLAnalyticsName = "myfirstdatalakeanalysis" 
$DLAnalyticsDoP = 10
$DLStoreName = "myfirstdatalakestore01"


#Create Azure Connection
Login-AzureRmAccount | Out-Null

$USQLFile = $WhereAmI + "RecursiveOutputPrep.usql"
$PrepOutput = $WhereAmI + "AmbulanceDataDateList.txt"

#Summit Job
$job = Submit-AzureRmDataLakeAnalyticsJob `
    -Name "GetDateList" `
    -AccountName $DLAnalyticsName `
    –ScriptPath $USQLFile `
    -DegreeOfParallelism $DLAnalyticsDoP

Write-Host "Submitted USQL prep job."

#Wait for job to complete
Wait-AdlJob -Account $DLAnalyticsName -JobId $job.JobId | Out-Null

Write-Host "Downloading USQL output file."

#Download date list
Export-AzureRmDataLakeStoreItem `
    -AccountName $DLStoreName `
    -Path $myrootdir\output\AmbulanceDataDateList.csv `
    -Destination $PrepOutput | Out-Null

Write-Host "Downloaded USQL output file."

#Read dates
$Dates = Get-Content $PrepOutput

Write-Host "Read date list."

#Loop over dates with proc call for each
ForEach ($Date in $Dates)
    {
    $USQLProcCall = '[dbo].[usp_OutputDailyAvgSpeed]("' + $Date + '");'
    $JobName = 'Output daily avg dataset for ' + $Date

    Write-Host $USQLProcCall

    $job = Submit-AzureRmDataLakeAnalyticsJob `
        -Name $JobName `
        -AccountName $DLAnalyticsName `
        –Script $USQLProcCall `
        -DegreeOfParallelism $DLAnalyticsDoP

    Write-Host "Job submitted for " $Date
    }

