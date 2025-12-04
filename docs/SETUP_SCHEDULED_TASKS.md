# Scheduled Tasks Setup - Complete Guide

## ✅ What Was Created

1. **Setup Script**: `scripts/schedule_forward_test_simple.ps1`
   - Creates Windows scheduled tasks
   - Simple command-line approach (more reliable than XML)

2. **Execution Scripts**:
   - `scripts/run_forward_test_predict.ps1` - Makes predictions for every supported league
   - `scripts/run_forward_test_update.ps1` - Refreshes results (and runs league-specific ingesters when available)

3. **Log Directory**: `logs/` - Stores daily execution logs

## 🚀 Quick Setup

### Step 1: Run Setup Script

Open PowerShell **as Administrator** (right-click → Run as Administrator):

```powershell
cd C:\Users\Bobby\Desktop\sports
.\scripts\schedule_forward_test_simple.ps1
```

**Note**: Administrator privileges are required to create scheduled tasks.

### Step 2: Verify Tasks Created

```powershell
# Check if tasks exist
schtasks /Query /TN "Forward Test - Predict"
schtasks /Query /TN "Forward Test - Update"
```

Or open Task Scheduler GUI:
- Press `Win + R`, type `taskschd.msc`, press Enter
- Look for tasks in the task list

## 📅 Task Schedule

### Task 1: Make Predictions
- **Name**: `Forward Test - Predict`
- **Time**: Daily at 11:00 PM UTC (6:00 PM EST / 7:00 PM EDT)
- **Purpose**: Runs predictions for NBA, NFL, CFB (and any future leagues defined in `SUPPORTED_LEAGUES`)
- **Logs**: `logs\forward_test_predict_YYYYMMDD.log`

### Task 2: Update Results
- **Name**: `Forward Test - Update`
- **Time**: Daily at 7:00 AM UTC (2:00 AM EST / 3:00 AM EDT)
- **Purpose**: Ingests the latest results (where supported) and updates the forward-test ledger
- **Logs**: `logs\forward_test_update_YYYYMMDD.log`

## 🎮 Manual Operations

### Run Tasks Manually

```powershell
# Make predictions now
schtasks /Run /TN "Forward Test - Predict"

# Update results now
schtasks /Run /TN "Forward Test - Update"
```

### View Task Details

```powershell
# View task information
schtasks /Query /TN "Forward Test - Predict" /V /FO LIST
schtasks /Query /TN "Forward Test - Update" /V /FO LIST
```

### Check Logs

```powershell
# View today's logs
Get-Content logs\forward_test_predict_$(Get-Date -Format 'yyyyMMdd').log
Get-Content logs\forward_test_update_$(Get-Date -Format 'yyyyMMdd').log

# View last 20 lines
Get-Content logs\forward_test_predict_$(Get-Date -Format 'yyyyMMdd').log -Tail 20
```

## ⚙️ Modify Schedule

### Change Times (via Task Scheduler GUI)

1. Open Task Scheduler (`taskschd.msc`)
2. Find the task (e.g., "Forward Test - Predict")
3. Right-click → Properties
4. Go to "Triggers" tab
5. Select trigger → Edit
6. Change time → OK → OK

### Change Times (via Command Line)

```powershell
# Change prediction time to 5:00 PM EST (22:00 UTC)
schtasks /Change /TN "Forward Test - Predict" /ST 22:00

# Change update time to 3:00 AM EST (08:00 UTC)
schtasks /Change /TN "Forward Test - Update" /ST 08:00
```

## 🗑️ Remove Tasks

### Remove All Tasks

```powershell
.\scripts\schedule_forward_test_simple.ps1 -Remove
```

### Remove Individual Tasks

```powershell
schtasks /Delete /TN "Forward Test - Predict" /F
schtasks /Delete /TN "Forward Test - Update" /F
```

## 🔧 Troubleshooting

### "Access Denied" Error

**Solution**: Run PowerShell as Administrator
- Right-click PowerShell → "Run as Administrator"
- Then run the setup script

### Tasks Not Running

1. **Check if enabled:**
   ```powershell
   schtasks /Query /TN "Forward Test - Predict" /V /FO LIST | Select-String "Status"
   ```

2. **Check last run result:**
   - Open Task Scheduler GUI
   - Find task → Check "Last Run Result" column
   - 0 = Success, other = Error code

3. **Check logs for errors:**
   ```powershell
   Get-Content logs\forward_test_predict_*.log -Tail 50
   ```

### "Execution Policy" Error

**Solution**: Set execution policy
```powershell
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Tasks Running but Failing

1. **Test manually first:**
   ```powershell
   poetry run python -m src.models.forward_test predict --dotenv .env
   poetry run python -m src.models.forward_test update
   ```

2. **Check .env file:**
   - Verify `ODDS_API_KEY` is set
   - Check file path is correct

3. **Check Poetry:**
   - Ensure Poetry is installed and in PATH
   - Test: `poetry --version`

### Logs Not Created

1. **Check directory exists:**
   ```powershell
   Test-Path logs
   ```

2. **Create if missing:**
   ```powershell
   New-Item -ItemType Directory -Path logs
   ```

3. **Check permissions:**
   - Ensure write access to project directory

## 📊 Monitoring

### Daily Check

```powershell
# Quick status check
Get-ChildItem logs\forward_test_*.log | Sort-Object LastWriteTime -Descending | Select-Object -First 2
```

### Weekly Review

```powershell
# View all logs from this week
Get-ChildItem logs\forward_test_*.log | Where-Object { $_.LastWriteTime -gt (Get-Date).AddDays(-7) }
```

### Generate Performance Report

```powershell
poetry run python -m src.models.forward_test report
```

## ⚠️ Important Notes

1. **Computer must be on**: Tasks only run if computer is awake
   - Consider preventing sleep during scheduled times
   - Or use "Wake computer to run this task" option

2. **Time zones**: Tasks use UTC time
   - 11:00 PM UTC = 6:00 PM EST (winter)
   - 11:00 PM UTC = 7:00 PM EDT (summer)
   - Windows converts automatically

3. **Network required**: Tasks require internet for API calls
   - "Run only if network available" is enabled

4. **User account**: Tasks run as your user account
   - Must stay logged in (or use "Run whether user is logged on or not")

## 🎯 Next Steps

1. ✅ Run setup script as Administrator
2. ✅ Verify tasks are created
3. ✅ Test by running tasks manually
4. ✅ Monitor logs for a few days
5. ✅ Adjust schedule if needed
6. ✅ Review weekly performance

Your forward testing is now fully automated! 🎉


