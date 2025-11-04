# Scheduled Tasks Setup Guide

## Overview

This guide explains how to set up automated scheduled tasks for forward testing. The tasks will automatically:
1. Make predictions on live games (daily at 6:00 PM EST)
2. Update predictions with results (daily at 2:00 AM EST)

## Quick Setup

### 1. Run the Setup Script

```powershell
cd C:\Users\Bobby\Desktop\sports
.\scripts\schedule_forward_test.ps1
```

This will:
- Create two Windows scheduled tasks
- Set them to run automatically at the specified times
- Configure logging and error handling

### 2. Verify Tasks Created

Open Task Scheduler:
- Press `Win + R`, type `taskschd.msc`, press Enter
- Look for tasks named:
  - `NBA Forward Test - Predict`
  - `NBA Forward Test - Update`

Or use command line:
```powershell
schtasks /Query /TN "NBA Forward Test - Predict"
schtasks /Query /TN "NBA Forward Test - Update"
```

## Task Details

### Task 1: Make Predictions
- **Name**: `NBA Forward Test - Predict`
- **Schedule**: Daily at 6:00 PM EST (11:00 PM UTC)
- **Action**: Runs `scripts\run_forward_test_predict.ps1`
- **Purpose**: Fetches live games and makes predictions before they start
- **Logs**: `logs\forward_test_predict_YYYYMMDD.log`

### Task 2: Update Results
- **Name**: `NBA Forward Test - Update`
- **Schedule**: Daily at 2:00 AM EST (7:00 AM UTC)
- **Action**: Runs `scripts\run_forward_test_update.ps1`
- **Purpose**: Updates predictions with actual game results after games finish
- **Logs**: `logs\forward_test_update_YYYYMMDD.log`

## Manual Operations

### Run Tasks Manually

```powershell
# Run predictions now
schtasks /Run /TN "NBA Forward Test - Predict"

# Update results now
schtasks /Run /TN "NBA Forward Test - Update"
```

### View Task Status

```powershell
# View all NBA forward test tasks
schtasks /Query /TN "NBA Forward Test - Predict" /V /FO LIST
schtasks /Query /TN "NBA Forward Test - Update" /V /FO LIST
```

### Check Logs

```powershell
# View today's prediction log
Get-Content logs\forward_test_predict_$(Get-Date -Format 'yyyyMMdd').log

# View today's update log
Get-Content logs\forward_test_update_$(Get-Date -Format 'yyyyMMdd').log
```

## Modifying Schedule

### Change Times

1. Open Task Scheduler (`taskschd.msc`)
2. Find the task you want to modify
3. Right-click → Properties → Triggers tab
4. Edit the trigger and change the time
5. Click OK

### Or Use Command Line

```powershell
# Change prediction time to 5:00 PM EST
schtasks /Change /TN "NBA Forward Test - Predict" /ST 17:00

# Change update time to 3:00 AM EST
schtasks /Change /TN "NBA Forward Test - Update" /ST 03:00
```

## Removing Tasks

### Remove All Tasks

```powershell
.\scripts\schedule_forward_test.ps1 -Remove
```

### Remove Individual Tasks

```powershell
schtasks /Delete /TN "NBA Forward Test - Predict" /F
schtasks /Delete /TN "NBA Forward Test - Update" /F
```

## Troubleshooting

### Task Not Running

1. **Check if task is enabled:**
   ```powershell
   schtasks /Query /TN "NBA Forward Test - Predict" /V /FO LIST | Select-String "Status"
   ```

2. **Check last run result:**
   - Open Task Scheduler
   - Find the task
   - Check "Last Run Result" column
   - 0 = Success, other numbers = Error code

3. **Check logs:**
   ```powershell
   Get-Content logs\forward_test_predict_*.log -Tail 50
   ```

### Task Running but Failing

1. **Check logs for errors:**
   - Look in `logs\` directory for error messages
   - Check if Poetry is in PATH
   - Verify .env file exists and has API key

2. **Test manually:**
   ```powershell
   poetry run python -m src.models.forward_test predict --dotenv .env
   ```

3. **Check permissions:**
   - Task must run as your user account
   - User must have permission to run PowerShell scripts
   - May need to set execution policy: `Set-ExecutionPolicy RemoteSigned -Scope CurrentUser`

### Logs Not Created

1. **Check if logs directory exists:**
   ```powershell
   Test-Path logs
   ```

2. **Create manually if needed:**
   ```powershell
   New-Item -ItemType Directory -Path logs
   ```

3. **Check write permissions:**
   - Ensure you have write access to the project directory

## Advanced Configuration

### Run on Specific Days Only

Edit the task in Task Scheduler:
1. Properties → Triggers → Edit
2. Change "Repeat task every" to "Weekly"
3. Select specific days of the week

### Add Email Notifications

You can modify the scripts to send email on errors:
1. Add email sending code to `run_forward_test_predict.ps1`
2. Use `Send-MailMessage` or similar

### Run Multiple Times Per Day

To check for games multiple times:
1. Create additional triggers in Task Scheduler
2. Or duplicate the task with different times

## Best Practices

1. **Monitor logs regularly** - Check logs at least weekly
2. **Test manually first** - Before scheduling, test commands manually
3. **Keep system awake** - Ensure computer doesn't sleep during scheduled times
4. **Backup data** - Forward test data is valuable, back it up regularly
5. **Review performance** - Run report command weekly to track performance

## Time Zone Notes

- Tasks are scheduled in UTC (11:00 PM UTC = 6:00 PM EST)
- Windows converts to your local time zone automatically
- EST is UTC-5, EDT is UTC-4 (Daylight Saving Time)
- Adjust times if you're in a different time zone

## Next Steps

1. ✅ Set up scheduled tasks using the setup script
2. ✅ Verify tasks are created and enabled
3. ✅ Test by running tasks manually
4. ✅ Monitor logs for a few days
5. ✅ Adjust schedule if needed
6. ✅ Review weekly performance reports

Your forward testing is now fully automated! 🎉


